package com.danubemessaging.client;

import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.consumer.TopicConsumer;
import com.danubemessaging.client.model.StreamMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Receives messages from a Danube topic and manages per-partition consumers.
 */
public final class Consumer implements AutoCloseable {
    private enum LifecycleState {
        NEW,
        SUBSCRIBED,
        FAILED,
        CLOSED
    }

    private final DanubeClient client;
    private final ConsumerOptions options;
    private final SubmissionPublisher<StreamMessage> publisher = new SubmissionPublisher<>();
    private final List<TopicConsumer> topicConsumers = new CopyOnWriteArrayList<>();
    private final List<Future<?>> receiveTasks = new CopyOnWriteArrayList<>();
    private final Map<String, TopicConsumer> consumerByTopic = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<LifecycleState> lifecycleState = new AtomicReference<>(LifecycleState.NEW);

    Consumer(DanubeClient client, ConsumerOptions options) {
        this.client = Objects.requireNonNull(client, "client");
        this.options = Objects.requireNonNull(options, "options");
    }

    public ConsumerOptions options() {
        return options;
    }

    public CompletableFuture<Void> subscribeAsync() {
        return CompletableFuture.runAsync(this::subscribe, client.ioExecutor());
    }

    public synchronized void subscribe() {
        ensureOpen();

        if (running.get() && lifecycleState.get() == LifecycleState.SUBSCRIBED) {
            return;
        }

        List<String> partitions = client.lookupService().topicPartitions(client.serviceUri(), options.topic());
        List<String> targets = partitions.isEmpty() ? List.of(options.topic()) : partitions;

        List<TopicConsumer> createdConsumers = new ArrayList<>(targets.size());
        try {
            for (int i = 0; i < targets.size(); i++) {
                String partitionTopic = targets.get(i);
                String partitionConsumerName = targets.size() == 1
                        ? options.consumerName()
                        : options.consumerName() + "-" + i;

                TopicConsumer topicConsumer = new TopicConsumer(
                        client.serviceUri(),
                        client.connectionManager(),
                        client.lookupService(),
                        client.authService(),
                        client.healthCheckService(),
                        client.retryManager(),
                        options,
                        partitionTopic,
                        partitionConsumerName);
                topicConsumer.subscribe();
                createdConsumers.add(topicConsumer);
                consumerByTopic.put(partitionTopic, topicConsumer);
            }
        } catch (RuntimeException error) {
            createdConsumers.forEach(TopicConsumer::close);
            consumerByTopic.entrySet().removeIf(entry -> createdConsumers.contains(entry.getValue()));
            throw error;
        }

        topicConsumers.addAll(createdConsumers);
        running.set(true);
        lifecycleState.set(LifecycleState.SUBSCRIBED);

        for (TopicConsumer topicConsumer : topicConsumers) {
            Future<?> receiveTask = client.ioExecutor()
                    .submit(() -> runReceiveLoop(topicConsumer));
            receiveTasks.add(receiveTask);
        }
    }

    public Flow.Publisher<StreamMessage> receive() {
        return publisher;
    }

    public CompletableFuture<Void> ackAsync(StreamMessage message) {
        return CompletableFuture.runAsync(() -> ack(message), client.ioExecutor());
    }

    public void ack(StreamMessage message) {
        ensureOpen();

        if (message == null) {
            throw new DanubeClientException("Message is required for ack");
        }

        TopicConsumer topicConsumer = consumerByTopic.get(message.messageId().topicName());
        if (topicConsumer == null) {
            throw new DanubeClientException(
                    "No consumer found for topic in message id: " + message.messageId().topicName());
        }

        topicConsumer.ack(message);
        notifyMessageAcked(topicConsumer, message);
    }

    @Override
    public synchronized void close() {
        if (lifecycleState.get() == LifecycleState.CLOSED) {
            return;
        }

        lifecycleState.set(LifecycleState.CLOSED);
        running.set(false);
        receiveTasks.forEach(task -> task.cancel(true));
        receiveTasks.clear();
        topicConsumers.forEach(TopicConsumer::close);
        topicConsumers.clear();
        consumerByTopic.clear();
        publisher.close();
    }

    private void runReceiveLoop(TopicConsumer topicConsumer) {
        try {
            topicConsumer.receiveLoop(message -> publish(topicConsumer, message), running::get);
        } catch (RuntimeException error) {
            failOnReceiveLoopError(topicConsumer, error);
        }
    }

    private synchronized void failOnReceiveLoopError(TopicConsumer topicConsumer, RuntimeException error) {
        if (lifecycleState.get() == LifecycleState.CLOSED || lifecycleState.get() == LifecycleState.FAILED) {
            return;
        }

        lifecycleState.set(LifecycleState.FAILED);
        running.set(false);
        receiveTasks.forEach(task -> task.cancel(true));
        receiveTasks.clear();
        topicConsumers.forEach(TopicConsumer::close);
        topicConsumers.clear();
        consumerByTopic.clear();
        notifyFatalReceiveError(topicConsumer, error);
        publisher.closeExceptionally(error);
    }

    private void publish(TopicConsumer topicConsumer, StreamMessage message) {
        if (message != null && lifecycleState.get() == LifecycleState.SUBSCRIBED) {
            notifyMessageReceived(topicConsumer, message);
            publisher.submit(message);
        }
    }

    private void notifyMessageReceived(TopicConsumer topicConsumer, StreamMessage message) {
        try {
            options.eventListener().onMessageReceived(topicConsumer.topic(), topicConsumer.consumerName(), message);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private void notifyMessageAcked(TopicConsumer topicConsumer, StreamMessage message) {
        try {
            options.eventListener().onMessageAcked(topicConsumer.topic(), topicConsumer.consumerName(), message);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private void notifyFatalReceiveError(TopicConsumer topicConsumer, RuntimeException error) {
        try {
            options.eventListener().onFatalReceiveError(topicConsumer.topic(), topicConsumer.consumerName(), error);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private void ensureOpen() {
        if (lifecycleState.get() == LifecycleState.CLOSED) {
            throw new DanubeClientException("Consumer is closed");
        }

        if (lifecycleState.get() == LifecycleState.FAILED) {
            throw new DanubeClientException("Consumer receive loop terminated due to a fatal error");
        }
    }
}
