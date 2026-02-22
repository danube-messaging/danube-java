package com.danubemessaging.client;

import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.consumer.TopicConsumer;
import com.danubemessaging.client.internal.retry.RetryManager;
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
 * Receives messages from a Danube topic.
 *
 * <p>Obtain an instance via {@link DanubeClient#newConsumer()}. Call {@link #subscribe()} to
 * register with the broker, then call {@link #receive()} to get the message stream.
 * Acknowledge each message with {@link #ack(StreamMessage)} to advance the subscription cursor.
 *
 * <p>This class is thread-safe.
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

    /**
     * Returns the options this consumer was built with.
     *
     * @return the consumer options
     */
    public ConsumerOptions options() {
        return options;
    }

    /**
     * Subscribes to the topic asynchronously.
     * Equivalent to calling {@link #subscribe()} on the IO executor.
     *
     * @return a future that completes when the subscription is established
     */
    public CompletableFuture<Void> subscribeAsync() {
        return CompletableFuture.runAsync(this::subscribe, client.ioExecutor());
    }

    /**
     * Subscribes to the topic and starts the background receive loop.
     * Must be called before {@link #receive()}.
     * Idempotent — calling twice on an already-subscribed consumer is a no-op.
     *
     * @throws com.danubemessaging.client.errors.DanubeClientException if subscription fails
     */
    public synchronized void subscribe() {
        ensureOpen();

        if (running.get() && lifecycleState.get() == LifecycleState.SUBSCRIBED) {
            return;
        }

        List<String> partitions = client.lookupService().topicPartitions(client.serviceUri(), options.topic());
        List<String> targets = partitions.isEmpty() ? List.of(options.topic()) : partitions;

        RetryManager retryManager = hasCustomRetryOptions()
                ? new RetryManager(options.maxRetries(), options.baseBackoffMs(), options.maxBackoffMs())
                : client.retryManager();

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
                        retryManager,
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

    /**
     * Returns a {@link Flow.Publisher} that emits incoming {@link StreamMessage} objects.
     * Subscribe to this publisher with a {@link Flow.Subscriber} to process messages.
     * The publisher completes exceptionally if the receive loop encounters a fatal error.
     *
     * @return the message publisher for this consumer
     */
    public Flow.Publisher<StreamMessage> receive() {
        return publisher;
    }

    /**
     * Acknowledges a message asynchronously.
     *
     * @param message the message to acknowledge
     * @return a future that completes when the ack is sent
     */
    public CompletableFuture<Void> ackAsync(StreamMessage message) {
        return CompletableFuture.runAsync(() -> ack(message), client.ioExecutor());
    }

    /**
     * Acknowledges a message, advancing the subscription cursor past it.
     * Must be called for each message to prevent redelivery.
     *
     * @param message the message to acknowledge; must not be null
     * @throws com.danubemessaging.client.errors.DanubeClientException if the message's topic
     *         has no associated consumer or the consumer is closed
     */
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

    /**
     * Closes this consumer, cancels the receive loop, and releases all resources.
     * Idempotent — safe to call multiple times.
     */
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

    private boolean hasCustomRetryOptions() {
        return options.maxRetries() > 0 || options.baseBackoffMs() > 0 || options.maxBackoffMs() > 0;
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
