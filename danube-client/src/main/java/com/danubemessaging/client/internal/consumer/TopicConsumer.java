package com.danubemessaging.client.internal.consumer;

import com.danubemessaging.client.ConsumerOptions;
import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.auth.AuthService;
import com.danubemessaging.client.internal.connection.BrokerAddress;
import com.danubemessaging.client.internal.connection.ConnectionManager;
import com.danubemessaging.client.internal.health.HealthCheckService;
import com.danubemessaging.client.internal.lookup.LookupService;
import com.danubemessaging.client.internal.retry.RetryManager;
import com.danubemessaging.client.model.StreamMessage;
import danube.ConsumerServiceGrpc;
import danube.DanubeApi;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Consumer bound to one concrete topic (or partition topic).
 */
public final class TopicConsumer {
    private enum LifecycleState {
        NEW,
        SUBSCRIBED,
        CLOSED
    }

    private final URI serviceUri;
    private final ConnectionManager connectionManager;
    private final LookupService lookupService;
    private final AuthService authService;
    private final HealthCheckService healthCheckService;
    private final RetryManager retryManager;
    private final ConsumerOptions options;
    private final String topic;
    private final String consumerName;
    private final AtomicLong requestId = new AtomicLong();
    private final AtomicReference<LifecycleState> lifecycleState = new AtomicReference<>(LifecycleState.NEW);

    private volatile BrokerAddress brokerAddress;
    private volatile long consumerId;

    public TopicConsumer(
            URI serviceUri,
            ConnectionManager connectionManager,
            LookupService lookupService,
            AuthService authService,
            HealthCheckService healthCheckService,
            RetryManager retryManager,
            ConsumerOptions options,
            String topic,
            String consumerName) {
        this.serviceUri = serviceUri;
        this.connectionManager = connectionManager;
        this.lookupService = lookupService;
        this.authService = authService;
        this.healthCheckService = healthCheckService;
        this.retryManager = retryManager;
        this.options = options;
        this.topic = topic;
        this.consumerName = consumerName;
    }

    public synchronized long subscribe() {
        ensureOpen();

        if (lifecycleState.get() == LifecycleState.SUBSCRIBED && consumerId != 0) {
            return consumerId;
        }

        brokerAddress = lookupService.handleLookup(serviceUri, topic);
        var connection = connectionManager.getConnection(brokerAddress.brokerUrl(), brokerAddress.connectUrl());

        var stub = ConsumerServiceGrpc.newBlockingStub(connection.grpcChannel())
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata()));

        DanubeApi.ConsumerRequest request = DanubeApi.ConsumerRequest.newBuilder()
                .setRequestId(requestId.incrementAndGet())
                .setTopicName(topic)
                .setConsumerName(consumerName)
                .setSubscription(options.subscription())
                .setSubscriptionType(options.subType().toProto())
                .build();

        try {
            DanubeApi.ConsumerResponse response = stub.subscribe(request);
            consumerId = response.getConsumerId();
            lifecycleState.set(LifecycleState.SUBSCRIBED);
            notifySubscribed(consumerId);
            return consumerId;
        } catch (Exception e) {
            notifyError(e, false);
            throw new DanubeClientException("Failed to subscribe consumer for topic: " + topic, e);
        }
    }

    public void receiveLoop(Consumer<StreamMessage> messageConsumer, java.util.function.BooleanSupplier isRunning) {
        int attempts = 0;

        while (isRunning.getAsBoolean() && lifecycleState.get() != LifecycleState.CLOSED) {
            if (consumerId == 0) {
                subscribe();
            }

            if (healthCheckService.shouldCloseConsumer(serviceUri, brokerAddress, consumerId)) {
                relookupAndResubscribe();
                continue;
            }

            var connection = connectionManager.getConnection(brokerAddress.brokerUrl(), brokerAddress.connectUrl());
            var stub = ConsumerServiceGrpc.newBlockingStub(connection.grpcChannel())
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata()));
            DanubeApi.ReceiveRequest request = DanubeApi.ReceiveRequest.newBuilder()
                    .setRequestId(requestId.incrementAndGet())
                    .setConsumerId(consumerId)
                    .build();

            try {
                var iterator = stub.receiveMessages(request);
                while (isRunning.getAsBoolean() && lifecycleState.get() != LifecycleState.CLOSED
                        && iterator.hasNext()) {
                    messageConsumer.accept(StreamMessage.fromProto(iterator.next()));
                }
                attempts = 0;
            } catch (RuntimeException error) {
                if (!isRunning.getAsBoolean() || lifecycleState.get() == LifecycleState.CLOSED) {
                    return;
                }

                if (retryManager.isUnrecoverable(error)) {
                    notifyError(error, true);
                    relookupAndResubscribe();
                    attempts = 0;
                    continue;
                }

                if (!retryManager.isRetryable(error)) {
                    notifyError(error, false);
                    throw error;
                }

                notifyError(error, true);

                attempts++;
                if (attempts > retryManager.maxRetries()) {
                    relookupAndResubscribe();
                    attempts = 0;
                    continue;
                }

                sleepBackoff(retryManager.calculateBackoff(attempts - 1));
            }
        }
    }

    public void ack(StreamMessage message) {
        int attempts = 0;
        while (true) {
            ensureOpen();

            if (consumerId == 0) {
                throw new DanubeClientException("Consumer is not subscribed for topic: " + topic);
            }

            var connection = connectionManager.getConnection(brokerAddress.brokerUrl(), brokerAddress.connectUrl());
            var stub = ConsumerServiceGrpc.newBlockingStub(connection.grpcChannel())
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata()));

            DanubeApi.AckRequest request = DanubeApi.AckRequest.newBuilder()
                    .setRequestId(requestId.incrementAndGet())
                    .setMsgId(message.messageId().toProto())
                    .setSubscriptionName(options.subscription())
                    .build();

            try {
                stub.ack(request);
                return;
            } catch (RuntimeException error) {
                if (retryManager.isUnrecoverable(error)) {
                    notifyError(error, true);
                    relookupAndResubscribe();
                    attempts = 0;
                    continue;
                }

                if (!retryManager.isRetryable(error)) {
                    notifyError(error, false);
                    throw new DanubeClientException("Failed to ack message for topic: " + topic, error);
                }

                notifyError(error, true);

                attempts++;
                if (attempts > retryManager.maxRetries()) {
                    relookupAndResubscribe();
                    attempts = 0;
                    continue;
                }

                sleepBackoff(retryManager.calculateBackoff(attempts - 1));
            }
        }
    }

    public synchronized void relookupAndResubscribe() {
        ensureOpen();
        consumerId = 0;
        subscribe();
    }

    public synchronized void close() {
        if (lifecycleState.get() == LifecycleState.CLOSED) {
            return;
        }

        lifecycleState.set(LifecycleState.CLOSED);
        consumerId = 0;
        notifyClosed();
    }

    public String topic() {
        return topic;
    }

    public String consumerName() {
        return consumerName;
    }

    private void ensureOpen() {
        if (lifecycleState.get() == LifecycleState.CLOSED) {
            throw new DanubeClientException("Topic consumer is closed: " + topic);
        }
    }

    private void notifySubscribed(long id) {
        try {
            options.eventListener().onConsumerSubscribed(topic, consumerName, id);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private void notifyClosed() {
        try {
            options.eventListener().onConsumerClosed(topic, consumerName);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private void notifyError(Throwable error, boolean retryable) {
        try {
            options.eventListener().onConsumerError(topic, consumerName, error, retryable);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private Metadata metadata() {
        Metadata metadata = new Metadata();
        authService.insertTokenIfNeeded(metadata, serviceUri);
        if (brokerAddress != null) {
            RetryManager.insertProxyHeader(metadata, brokerAddress.brokerUrl().toString(), brokerAddress.proxy());
        }
        return metadata;
    }

    private static void sleepBackoff(java.time.Duration backoff) {
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DanubeClientException("Interrupted while backing off for retry", interrupted);
        }
    }
}
