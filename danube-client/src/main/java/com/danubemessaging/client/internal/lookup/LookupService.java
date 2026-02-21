package com.danubemessaging.client.internal.lookup;

import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.auth.AuthService;
import com.danubemessaging.client.internal.connection.BrokerAddress;
import com.danubemessaging.client.internal.connection.ConnectionManager;
import com.danubemessaging.client.model.LookupResult;
import com.danubemessaging.client.model.LookupType;
import danube.DanubeApi;
import danube.DiscoveryGrpc;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performs topic owner and partition discovery.
 */
public final class LookupService {
    private final ConnectionManager connectionManager;
    private final AuthService authService;
    private final AtomicLong requestId = new AtomicLong();

    public LookupService(ConnectionManager connectionManager, AuthService authService) {
        this.connectionManager = connectionManager;
        this.authService = authService;
    }

    public LookupResult lookupTopic(URI address, String topic) {
        var grpcConnection = connectionManager.getConnection(address, address);
        var stub = authService.attachAuthIfNeeded(
                DiscoveryGrpc.newBlockingStub(grpcConnection.grpcChannel()),
                address);

        var request = DanubeApi.TopicLookupRequest.newBuilder()
                .setRequestId(requestId.incrementAndGet())
                .setTopic(topic)
                .build();

        try {
            var response = stub.topicLookup(request);
            return new LookupResult(
                    LookupType.fromProtoNumber(response.getResponseTypeValue()),
                    URI.create(response.getBrokerUrl()),
                    URI.create(response.getConnectUrl()),
                    response.getProxy());
        } catch (Exception e) {
            throw new DanubeClientException("Topic lookup failed for topic: " + topic, e);
        }
    }

    public List<String> topicPartitions(URI address, String topic) {
        var grpcConnection = connectionManager.getConnection(address, address);
        var stub = authService.attachAuthIfNeeded(
                DiscoveryGrpc.newBlockingStub(grpcConnection.grpcChannel()),
                address);

        var request = DanubeApi.TopicLookupRequest.newBuilder()
                .setRequestId(requestId.incrementAndGet())
                .setTopic(topic)
                .build();

        try {
            return stub.topicPartitions(request).getPartitionsList();
        } catch (Exception e) {
            throw new DanubeClientException("Topic partitions lookup failed for topic: " + topic, e);
        }
    }

    public BrokerAddress handleLookup(URI address, String topic) {
        LookupResult result = lookupTopic(address, topic);
        return switch (result.responseType()) {
            case REDIRECT, CONNECT ->
                    new BrokerAddress(result.connectUrl(), result.brokerUrl(), result.proxy());
            case FAILED -> throw new DanubeClientException(
                    "Topic lookup failed: topic may not exist or cluster is unavailable");
            case UNKNOWN -> throw new DanubeClientException("Unknown lookup response type");
        };
    }
}
