package com.danubemessaging.client;

import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.auth.AuthService;
import com.danubemessaging.client.internal.connection.ConnectionManager;
import com.danubemessaging.client.internal.connection.ConnectionOptions;
import com.danubemessaging.client.internal.health.HealthCheckService;
import com.danubemessaging.client.internal.lookup.LookupService;
import com.danubemessaging.client.internal.retry.RetryManager;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * Builder for {@link DanubeClient}.
 */
public final class DanubeClientBuilder {
    private String serviceUrl;
    private boolean useTls;
    private Path caCertPath;
    private Path clientCertPath;
    private Path clientKeyPath;
    private String apiKey;

    DanubeClientBuilder() {
    }

    public DanubeClientBuilder serviceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
        return this;
    }

    public DanubeClientBuilder withTls(Path caCertPath) {
        this.useTls = true;
        this.caCertPath = caCertPath;
        return this;
    }

    public DanubeClientBuilder withMutualTls(Path caCertPath, Path clientCertPath, Path clientKeyPath) {
        this.useTls = true;
        this.caCertPath = caCertPath;
        this.clientCertPath = clientCertPath;
        this.clientKeyPath = clientKeyPath;
        return this;
    }

    public DanubeClientBuilder withApiKey(String apiKey) {
        this.apiKey = apiKey;
        this.useTls = true;
        return this;
    }

    public DanubeClient build() {
        URI uri = parseServiceUri(serviceUrl);
        ConnectionOptions options = new ConnectionOptions(
                useTls,
                Optional.ofNullable(caCertPath),
                Optional.ofNullable(clientCertPath),
                Optional.ofNullable(clientKeyPath),
                Optional.ofNullable(apiKey));

        ConnectionManager connectionManager = new ConnectionManager(options);
        AuthService authService = new AuthService(connectionManager, options);

        if (apiKey != null && !apiKey.isBlank()) {
            authService.authenticateClient(uri, apiKey);
        }

        LookupService lookupService = new LookupService(connectionManager, authService);
        RetryManager retryManager = new RetryManager(0, 0, 0);
        HealthCheckService healthCheckService = new HealthCheckService(connectionManager, authService);

        return new DanubeClient(
                uri,
                connectionManager,
                authService,
                lookupService,
                retryManager,
                healthCheckService,
                Executors.newVirtualThreadPerTaskExecutor());
    }

    private static URI parseServiceUri(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new DanubeClientException("serviceUrl is required");
        }

        String normalized = raw.contains("://") ? raw : "http://" + raw;
        URI uri = URI.create(normalized);

        if (uri.getHost() == null) {
            throw new DanubeClientException("Invalid serviceUrl, host is required: " + raw);
        }

        return uri;
    }
}
