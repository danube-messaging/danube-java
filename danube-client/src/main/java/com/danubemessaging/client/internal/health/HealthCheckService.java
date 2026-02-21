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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes health checks for active producer/consumer instances.
 *
 * <p>
 * Background checks run on virtual threads with a 5-second interval,
 * matching the Go/Rust client behavior.
 */
public final class HealthCheckService {
    private static final Duration HEALTH_CHECK_INTERVAL = Duration.ofSeconds(5);

    private final ConnectionManager connectionManager;
    private final AuthService authService;
    private final AtomicLong requestId = new AtomicLong();

    public HealthCheckService(ConnectionManager connectionManager, AuthService authService) {
        this.connectionManager = connectionManager;
        this.authService = authService;
    }

    /**
     * Starts a background health check on a virtual thread that pings the broker
     * every 5 seconds. Sets {@code stopSignal} to {@code true} when the broker
     * responds with CLOSE.
     *
     * @return the virtual thread running the health check loop; interrupt it to
     *         stop.
     */
    public Thread startBackgroundHealthCheck(
            URI serviceUri,
            BrokerAddress brokerAddress,
            DanubeApi.HealthCheckRequest.ClientType clientType,
            long clientId,
            AtomicBoolean stopSignal) {
        Thread vt = Thread.ofVirtual()
                .name("danube-health-check-" + clientType.name().toLowerCase() + "-" + clientId)
                .start(() -> healthCheckLoop(serviceUri, brokerAddress, clientType, clientId, stopSignal));
        return vt;
    }

    private void healthCheckLoop(
            URI serviceUri,
            BrokerAddress brokerAddress,
            DanubeApi.HealthCheckRequest.ClientType clientType,
            long clientId,
            AtomicBoolean stopSignal) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(HEALTH_CHECK_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            boolean close = shouldClose(serviceUri, brokerAddress, clientType, clientId);
            if (close) {
                stopSignal.set(true);
                return;
            }
        }
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
