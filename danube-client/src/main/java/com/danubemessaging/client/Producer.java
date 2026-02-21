package com.danubemessaging.client;

import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.producer.TopicProducer;
import com.danubemessaging.client.internal.retry.RetryManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sends messages to a Danube topic and manages per-partition topic producers.
 */
public final class Producer {
    private enum LifecycleState {
        NEW,
        CREATED,
        CLOSED
    }

    private final DanubeClient client;
    private final ProducerOptions options;
    private final List<TopicProducer> topicProducers = new ArrayList<>();
    private final AtomicInteger nextPartition = new AtomicInteger();
    private final AtomicReference<LifecycleState> lifecycleState = new AtomicReference<>(LifecycleState.NEW);

    Producer(DanubeClient client, ProducerOptions options) {
        this.client = Objects.requireNonNull(client, "client");
        this.options = Objects.requireNonNull(options, "options");
    }

    public ProducerOptions options() {
        return options;
    }

    public CompletableFuture<Void> createAsync() {
        return CompletableFuture.runAsync(this::create, client.ioExecutor());
    }

    public synchronized void create() {
        ensureOpen();

        if (lifecycleState.get() == LifecycleState.CREATED) {
            return;
        }

        List<String> targets;
        if (options.partitions() > 0) {
            targets = new ArrayList<>(options.partitions());
            for (int i = 0; i < options.partitions(); i++) {
                targets.add(options.topic() + "-part-" + i);
            }
        } else {
            List<String> partitions = client.lookupService().topicPartitions(client.serviceUri(), options.topic());
            targets = partitions.isEmpty() ? List.of(options.topic()) : partitions;
        }

        SchemaRegistryClient schemaRegistry = options.schemaReference() != null
                ? client.newSchemaRegistry()
                : null;

        RetryManager retryManager = hasCustomRetryOptions()
                ? new RetryManager(options.maxRetries(), options.baseBackoffMs(), options.maxBackoffMs())
                : client.retryManager();

        try {
            for (int i = 0; i < targets.size(); i++) {
                String partitionTopic = targets.get(i);
                String partitionProducerName = targets.size() == 1
                        ? options.producerName()
                        : options.producerName() + "-" + i;

                TopicProducer topicProducer = new TopicProducer(
                        client.serviceUri(),
                        client.connectionManager(),
                        client.lookupService(),
                        client.authService(),
                        client.healthCheckService(),
                        schemaRegistry,
                        retryManager,
                        options,
                        partitionTopic,
                        partitionProducerName);
                try {
                    topicProducer.create();
                } catch (RuntimeException error) {
                    notifyProducerError(topicProducer, error, false);
                    throw error;
                }
                topicProducers.add(topicProducer);
            }
        } catch (RuntimeException error) {
            topicProducers.forEach(TopicProducer::close);
            topicProducers.clear();
            throw error;
        }

        lifecycleState.set(LifecycleState.CREATED);
    }

    public CompletableFuture<Long> sendAsync(byte[] payload, Map<String, String> attributes) {
        byte[] payloadCopy = payload == null ? new byte[0] : payload.clone();
        Map<String, String> attr = attributes == null ? Map.of() : Map.copyOf(attributes);
        return CompletableFuture.supplyAsync(() -> send(payloadCopy, attr), client.ioExecutor());
    }

    public long send(byte[] payload, Map<String, String> attributes) {
        ensureOpen();

        if (lifecycleState.get() != LifecycleState.CREATED) {
            create();
        }

        TopicProducer topicProducer = selectTopicProducer();
        int attempts = 0;

        while (true) {
            try {
                return topicProducer.send(payload, attributes);
            } catch (RuntimeException error) {
                boolean unrecoverable = client.retryManager().isUnrecoverable(error);
                if (unrecoverable) {
                    notifyProducerError(topicProducer, error, true);
                    topicProducer.relookupAndCreate();
                    attempts = 0;
                    continue;
                }

                boolean retryable = client.retryManager().isRetryable(error);
                if (!retryable) {
                    notifyProducerError(topicProducer, error, false);
                    throw error;
                }

                notifyProducerError(topicProducer, error, true);

                attempts++;
                if (attempts > client.retryManager().maxRetries()) {
                    topicProducer.relookupAndCreate();
                    attempts = 0;
                    continue;
                }

                sleepBackoff(client.retryManager().calculateBackoff(attempts - 1));
            }
        }
    }

    public synchronized void close() {
        if (lifecycleState.get() == LifecycleState.CLOSED) {
            return;
        }

        lifecycleState.set(LifecycleState.CLOSED);
        topicProducers.forEach(TopicProducer::close);
        topicProducers.clear();
    }

    private void ensureOpen() {
        if (lifecycleState.get() == LifecycleState.CLOSED) {
            throw new DanubeClientException("Producer is closed");
        }
    }

    private void notifyProducerError(TopicProducer topicProducer, Throwable error, boolean retryable) {
        try {
            options.eventListener().onProducerError(topicProducer.topic(), topicProducer.producerName(), error,
                    retryable);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private TopicProducer selectTopicProducer() {
        if (topicProducers.isEmpty()) {
            throw new DanubeClientException("Producer is not initialized");
        }

        if (topicProducers.size() == 1) {
            return topicProducers.get(0);
        }

        int index = Math.floorMod(nextPartition.getAndIncrement(), topicProducers.size());
        return topicProducers.get(index);
    }

    private boolean hasCustomRetryOptions() {
        return options.maxRetries() > 0 || options.baseBackoffMs() > 0 || options.maxBackoffMs() > 0;
    }

    private static void sleepBackoff(Duration backoff) {
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DanubeClientException("Interrupted while backing off for retry", interrupted);
        }
    }
}
