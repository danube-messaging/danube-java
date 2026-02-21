package com.danubemessaging.client.internal.producer;

import com.danubemessaging.client.ProducerOptions;
import com.danubemessaging.client.SchemaRegistryClient;
import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.auth.AuthService;
import com.danubemessaging.client.internal.connection.BrokerAddress;
import com.danubemessaging.client.internal.connection.ConnectionManager;
import com.danubemessaging.client.internal.health.HealthCheckService;
import com.danubemessaging.client.internal.lookup.LookupService;
import com.danubemessaging.client.internal.retry.RetryManager;
import com.danubemessaging.client.schema.SchemaInfo;
import com.danubemessaging.client.schema.SchemaReference;
import com.google.protobuf.ByteString;
import danube.DanubeApi;
import danube.ProducerServiceGrpc;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Producer bound to one concrete topic (or partition topic).
 */
public final class TopicProducer {
    private enum LifecycleState {
        NEW,
        CREATED,
        CLOSED
    }

    private final URI serviceUri;
    private final ConnectionManager connectionManager;
    private final LookupService lookupService;
    private final AuthService authService;
    private final HealthCheckService healthCheckService;
    private final SchemaRegistryClient schemaRegistryClient;
    private final RetryManager retryManager;
    private final ProducerOptions options;
    private final String topic;
    private final String producerName;
    private final AtomicLong requestId = new AtomicLong();
    private final AtomicReference<LifecycleState> lifecycleState = new AtomicReference<>(LifecycleState.NEW);

    private volatile BrokerAddress brokerAddress;
    private volatile long producerId;
    private volatile Long cachedSchemaId;
    private volatile Integer cachedSchemaVersion;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);
    private volatile Thread healthCheckThread;

    public TopicProducer(
            URI serviceUri,
            ConnectionManager connectionManager,
            LookupService lookupService,
            AuthService authService,
            HealthCheckService healthCheckService,
            SchemaRegistryClient schemaRegistryClient,
            RetryManager retryManager,
            ProducerOptions options,
            String topic,
            String producerName) {
        this.serviceUri = serviceUri;
        this.connectionManager = connectionManager;
        this.lookupService = lookupService;
        this.authService = authService;
        this.healthCheckService = healthCheckService;
        this.schemaRegistryClient = schemaRegistryClient;
        this.retryManager = retryManager;
        this.options = options;
        this.topic = topic;
        this.producerName = producerName;
    }

    public synchronized long create() {
        ensureOpen();

        if (lifecycleState.get() == LifecycleState.CREATED && producerId != 0) {
            return producerId;
        }

        brokerAddress = lookupService.tryLookup(serviceUri, topic);
        int attempts = 0;

        while (true) {
            try {
                return tryCreate();
            } catch (RuntimeException error) {
                if (!retryManager.isRetryable(error)) {
                    throw error;
                }
                attempts++;
                if (attempts > retryManager.maxRetries()) {
                    throw error;
                }
                brokerAddress = lookupService.tryLookup(serviceUri, topic);
                sleepBackoff(retryManager.calculateBackoff(attempts - 1));
            }
        }
    }

    private long tryCreate() {
        var connection = connectionManager.getConnection(brokerAddress.brokerUrl(), brokerAddress.connectUrl());

        var stub = ProducerServiceGrpc.newBlockingStub(connection.grpcChannel());
        stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata()));

        DanubeApi.ProducerRequest.Builder requestBuilder = DanubeApi.ProducerRequest.newBuilder()
                .setRequestId(requestId.incrementAndGet())
                .setProducerName(producerName)
                .setTopicName(topic)
                .setProducerAccessMode(options.accessMode().toProto())
                .setDispatchStrategy(options.dispatchStrategy().toProto());

        if (options.schemaReference() != null) {
            requestBuilder.setSchemaRef(options.schemaReference().toProto());
        }

        DanubeApi.ProducerRequest request = requestBuilder.build();

        try {
            DanubeApi.ProducerResponse response = stub.createProducer(request);
            producerId = response.getProducerId();

            if (options.schemaReference() != null) {
                resolveSchemaMetadata(options.schemaReference());
            }

            startBackgroundHealthCheck();
            lifecycleState.set(LifecycleState.CREATED);
            notifyProducerCreated(producerId);
            return producerId;
        } catch (Exception e) {
            throw new DanubeClientException("Failed to create producer for topic: " + topic, e);
        }
    }

    public long send(byte[] payload, Map<String, String> attributes) {
        ensureOpen();

        if (lifecycleState.get() != LifecycleState.CREATED || producerId == 0) {
            create();
        }

        if (stopSignal.compareAndSet(true, false)) {
            relookupAndCreate();
        }

        var connection = connectionManager.getConnection(brokerAddress.brokerUrl(), brokerAddress.connectUrl());
        var stub = ProducerServiceGrpc.newBlockingStub(connection.grpcChannel());
        stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata()));

        DanubeApi.MsgID msgId = DanubeApi.MsgID.newBuilder()
                .setProducerId(producerId)
                .setTopicName(topic)
                .setBrokerAddr(brokerAddress.brokerUrl().toString())
                .build();

        DanubeApi.StreamMessage.Builder messageBuilder = DanubeApi.StreamMessage.newBuilder()
                .setRequestId(requestId.incrementAndGet())
                .setMsgId(msgId)
                .setPayload(ByteString.copyFrom(payload))
                .setPublishTime(System.currentTimeMillis())
                .setProducerName(producerName)
                .putAllAttributes(attributes);

        if (cachedSchemaId != null) {
            messageBuilder.setSchemaId(cachedSchemaId);
        }
        if (cachedSchemaVersion != null) {
            messageBuilder.setSchemaVersion(cachedSchemaVersion);
        }

        DanubeApi.StreamMessage message = messageBuilder.build();

        try {
            DanubeApi.MessageResponse response = stub.sendMessage(message);
            long sentRequestId = response.getRequestId();
            notifyMessageSent(sentRequestId, payload.length, attributes.size());
            return sentRequestId;
        } catch (Exception e) {
            throw new DanubeClientException("Failed to send message for topic: " + topic, e);
        }
    }

    public String topic() {
        return topic;
    }

    public String producerName() {
        return producerName;
    }

    public synchronized void relookupAndCreate() {
        ensureOpen();
        cancelHealthCheckTask();
        producerId = 0;
        create();
    }

    public synchronized void close() {
        if (lifecycleState.get() == LifecycleState.CLOSED) {
            return;
        }

        cancelHealthCheckTask();
        lifecycleState.set(LifecycleState.CLOSED);
        producerId = 0;
        notifyProducerClosed();
    }

    private void ensureOpen() {
        if (lifecycleState.get() == LifecycleState.CLOSED) {
            throw new DanubeClientException("Topic producer is closed: " + topic);
        }
    }

    private void notifyProducerCreated(long id) {
        try {
            options.eventListener().onProducerCreated(topic, producerName, id);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private void notifyMessageSent(long requestIdValue, int payloadBytes, int attributeCount) {
        try {
            options.eventListener().onMessageSent(
                    topic,
                    producerName,
                    requestIdValue,
                    payloadBytes,
                    attributeCount);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private void notifyProducerClosed() {
        try {
            options.eventListener().onProducerClosed(topic, producerName);
        } catch (RuntimeException ignore) {
            // Listener errors must never break client flow.
        }
    }

    private void resolveSchemaMetadata(SchemaReference schemaRef) {
        if (schemaRegistryClient == null) {
            throw new DanubeClientException(
                    "Schema reference is set but no schema registry client is available for topic: " + topic);
        }

        if (schemaRef.useLatest()) {
            SchemaInfo latest = schemaRegistryClient.getLatestSchema(schemaRef.subject());
            cachedSchemaId = latest.schemaId();
            cachedSchemaVersion = latest.version();
        } else if (schemaRef.pinnedVersion() != null) {
            SchemaInfo latest = schemaRegistryClient.getLatestSchema(schemaRef.subject());
            int pinned = schemaRef.pinnedVersion();
            if (pinned > latest.version()) {
                throw new DanubeClientException(
                        "Pinned version " + pinned + " does not exist for subject '" + schemaRef.subject()
                                + "'. Latest version is " + latest.version());
            }
            if (pinned == latest.version()) {
                cachedSchemaId = latest.schemaId();
                cachedSchemaVersion = latest.version();
            } else {
                SchemaInfo pinnedSchema = schemaRegistryClient.getSchemaById(latest.schemaId(), pinned);
                cachedSchemaId = pinnedSchema.schemaId();
                cachedSchemaVersion = pinnedSchema.version();
            }
        } else if (schemaRef.minVersion() != null) {
            SchemaInfo latest = schemaRegistryClient.getLatestSchema(schemaRef.subject());
            int min = schemaRef.minVersion();
            if (latest.version() < min) {
                throw new DanubeClientException(
                        "Latest version " + latest.version() + " does not meet minimum version requirement "
                                + min + " for subject '" + schemaRef.subject() + "'");
            }
            cachedSchemaId = latest.schemaId();
            cachedSchemaVersion = latest.version();
        }
    }

    private void startBackgroundHealthCheck() {
        cancelHealthCheckTask();
        stopSignal.set(false);
        healthCheckThread = healthCheckService.startBackgroundHealthCheck(
                serviceUri,
                brokerAddress,
                DanubeApi.HealthCheckRequest.ClientType.Producer,
                producerId,
                stopSignal);
    }

    private void cancelHealthCheckTask() {
        Thread t = healthCheckThread;
        if (t != null) {
            t.interrupt();
            healthCheckThread = null;
        }
    }

    private Metadata metadata() {
        Metadata metadata = new Metadata();
        authService.insertTokenIfNeeded(metadata, serviceUri);
        RetryManager.insertProxyHeader(metadata, brokerAddress.brokerUrl().toString(), brokerAddress.proxy());
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
