package com.danubemessaging.client.internal.health;

import com.danubemessaging.client.internal.auth.AuthService;
import com.danubemessaging.client.internal.connection.BrokerAddress;
import com.danubemessaging.client.internal.connection.ConnectionManager;
import com.danubemessaging.client.internal.retry.RetryManager;
import danube.DanubeApi;
import danube.HealthCheckGrpc;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes health checks for active producer/consumer instances.
 */
public final class HealthCheckService {
    private final ConnectionManager connectionManager;
    private final AuthService authService;
    private final AtomicLong requestId = new AtomicLong();

    public HealthCheckService(ConnectionManager connectionManager, AuthService authService) {
        this.connectionManager = connectionManager;
        this.authService = authService;
    }

    public boolean shouldCloseProducer(URI serviceUri, BrokerAddress brokerAddress, long producerId) {
        return shouldClose(
                serviceUri,
                brokerAddress,
                DanubeApi.HealthCheckRequest.ClientType.Producer,
                producerId);
    }

    public boolean shouldCloseConsumer(URI serviceUri, BrokerAddress brokerAddress, long consumerId) {
        return shouldClose(
                serviceUri,
                brokerAddress,
                DanubeApi.HealthCheckRequest.ClientType.Consumer,
                consumerId);
    }

    private boolean shouldClose(
            URI serviceUri,
            BrokerAddress brokerAddress,
            DanubeApi.HealthCheckRequest.ClientType clientType,
            long id) {
        if (brokerAddress == null || id <= 0) {
            return false;
        }

        var connection = connectionManager.getConnection(brokerAddress.brokerUrl(), brokerAddress.connectUrl());
        var stub = HealthCheckGrpc.newBlockingStub(connection.grpcChannel())
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata(serviceUri, brokerAddress)));

        DanubeApi.HealthCheckRequest request = DanubeApi.HealthCheckRequest.newBuilder()
                .setRequestId(requestId.incrementAndGet())
                .setClient(clientType)
                .setId(id)
                .build();

        try {
            DanubeApi.HealthCheckResponse response = stub.healthCheck(request);
            return response.getStatus() == DanubeApi.HealthCheckResponse.ClientStatus.CLOSE;
        } catch (RuntimeException ignore) {
            return false;
        }
    }

    private Metadata metadata(URI serviceUri, BrokerAddress brokerAddress) {
        Metadata metadata = new Metadata();
        authService.insertTokenIfNeeded(metadata, serviceUri);
        RetryManager.insertProxyHeader(metadata, brokerAddress.brokerUrl().toString(), brokerAddress.proxy());
        return metadata;
    }
}
