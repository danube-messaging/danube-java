package com.danubemessaging.client.internal.connection;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Connection settings used when opening gRPC channels.
 */
public record ConnectionOptions(
        boolean useTls,
        Optional<Path> caCertPath,
        Optional<Path> clientCertPath,
        Optional<Path> clientKeyPath,
        Optional<String> apiKey) {

    public static ConnectionOptions plainText() {
        return new ConnectionOptions(
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public boolean isMutualTls() {
        return clientCertPath.isPresent() && clientKeyPath.isPresent();
    }
}
