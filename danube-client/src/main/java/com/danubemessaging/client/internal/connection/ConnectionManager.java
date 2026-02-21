package com.danubemessaging.client.internal.connection;

import com.danubemessaging.client.errors.DanubeClientException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches gRPC channels by broker/connect identity.
 */
public final class ConnectionManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

    private final ConcurrentMap<BrokerAddress, RpcConnection> connections = new ConcurrentHashMap<>();
    private final ConnectionOptions connectionOptions;

    public ConnectionManager(ConnectionOptions connectionOptions) {
        this.connectionOptions = connectionOptions;
    }

    public RpcConnection getConnection(URI brokerUrl, URI connectUrl) {
        boolean proxy = !brokerUrl.equals(connectUrl);
        BrokerAddress address = new BrokerAddress(connectUrl, brokerUrl, proxy);
        return connections.computeIfAbsent(address, this::newRpcConnection);
    }

    public ConnectionOptions connectionOptions() {
        return connectionOptions;
    }

    @Override
    public void close() {
        connections.values().forEach(connection -> connection.grpcChannel().shutdown());
        connections.clear();
    }

    private RpcConnection newRpcConnection(BrokerAddress address) {
        String target = toTarget(address.connectUrl());
        LOG.info("Establishing new gRPC connection to {}", target);

        try {
            ManagedChannel channel;
            if (!connectionOptions.useTls()) {
                channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
            } else {
                channel = buildTlsChannel(target);
            }
            return new RpcConnection(channel);
        } catch (Exception e) {
            throw new DanubeClientException("Failed to create gRPC connection to " + target, e);
        }
    }

    private ManagedChannel buildTlsChannel(String target) throws Exception {
        if (connectionOptions.caCertPath().isPresent() || connectionOptions.isMutualTls()) {
            SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
            connectionOptions.caCertPath().ifPresent(path -> sslContextBuilder.trustManager(path.toFile()));
            if (connectionOptions.isMutualTls()) {
                sslContextBuilder.keyManager(
                        connectionOptions.clientCertPath().orElseThrow().toFile(),
                        connectionOptions.clientKeyPath().orElseThrow().toFile());
            }
            return NettyChannelBuilder.forTarget(target)
                    .sslContext(sslContextBuilder.build())
                    .build();
        }

        return NettyChannelBuilder.forTarget(target).useTransportSecurity().build();
    }

    private static String toTarget(URI uri) {
        if (uri.getHost() != null) {
            if (uri.getPort() > 0) {
                return uri.getHost() + ":" + uri.getPort();
            }
            return uri.getHost();
        }
        return uri.toString();
    }
}
