package com.danubemessaging.client.internal.producer;

import com.danubemessaging.client.ProducerOptions;
import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.auth.AuthService;
import com.danubemessaging.client.internal.connection.BrokerAddress;
import com.danubemessaging.client.internal.connection.ConnectionManager;
import com.danubemessaging.client.internal.health.HealthCheckService;
import com.danubemessaging.client.internal.lookup.LookupService;
import com.danubemessaging.client.internal.retry.RetryManager;
import com.google.protobuf.ByteString;
import danube.DanubeApi;
import danube.ProducerServiceGrpc;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.net.URI;
import java.util.Map;
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
    private final ProducerOptions options;
    private final String topic;
    private final String producerName;
    private final AtomicLong requestId = new AtomicLong();
    private final AtomicReference<LifecycleState> lifecycleState = new AtomicReference<>(LifecycleState.NEW);

    private volatile BrokerAddress brokerAddress;
    private volatile long producerId;

    public TopicProducer(
            URI serviceUri,
            ConnectionManager connectionManager,
            LookupService lookupService,
            AuthService authService,
            HealthCheckService healthCheckService,
            ProducerOptions options,
            String topic,
            String producerName) {
        this.serviceUri = serviceUri;
        this.connectionManager = connectionManager;
        this.lookupService = lookupService;
        this.authService = authService;
        this.healthCheckService = healthCheckService;
        this.options = options;
        this.topic = topic;
        this.producerName = producerName;
    }

    public synchronized long create() {
        ensureOpen();

        if (lifecycleState.get() == LifecycleState.CREATED && producerId != 0) {
            return producerId;
        }

        brokerAddress = lookupService.handleLookup(serviceUri, topic);
        var connection = connectionManager.getConnection(brokerAddress.brokerUrl(), brokerAddress.connectUrl());

        var stub = ProducerServiceGrpc.newBlockingStub(connection.grpcChannel());
        stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata()));

        DanubeApi.ProducerRequest request = DanubeApi.ProducerRequest.newBuilder()
                .setRequestId(requestId.incrementAndGet())
                .setProducerName(producerName)
                .setTopicName(topic)
                .setProducerAccessMode(options.accessMode().toProto())
                .setDispatchStrategy(options.dispatchStrategy().toProto())
                .build();

        try {
            DanubeApi.ProducerResponse response = stub.createProducer(request);
            producerId = response.getProducerId();
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

        if (healthCheckService.shouldCloseProducer(serviceUri, brokerAddress, producerId)) {
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

        DanubeApi.StreamMessage message = DanubeApi.StreamMessage.newBuilder()
                .setRequestId(requestId.incrementAndGet())
                .setMsgId(msgId)
                .setPayload(ByteString.copyFrom(payload))
                .setPublishTime(System.currentTimeMillis())
                .setProducerName(producerName)
                .putAllAttributes(attributes)
                .build();

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
        producerId = 0;
        create();
    }

    public synchronized void close() {
        if (lifecycleState.get() == LifecycleState.CLOSED) {
            return;
        }

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

    private Metadata metadata() {
        Metadata metadata = new Metadata();
        authService.insertTokenIfNeeded(metadata, serviceUri);
        RetryManager.insertProxyHeader(metadata, brokerAddress.brokerUrl().toString(), brokerAddress.proxy());
        return metadata;
    }
}
