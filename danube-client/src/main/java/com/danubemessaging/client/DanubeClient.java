package com.danubemessaging.client;

import com.danubemessaging.client.internal.auth.AuthService;
import com.danubemessaging.client.internal.connection.ConnectionManager;
import com.danubemessaging.client.internal.health.HealthCheckService;
import com.danubemessaging.client.internal.lookup.LookupService;
import com.danubemessaging.client.internal.retry.RetryManager;
import com.danubemessaging.client.model.LookupResult;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Main entry point for interacting with Danube.
 */
public final class DanubeClient implements AutoCloseable {
    private final URI serviceUri;
    private final ConnectionManager connectionManager;
    private final AuthService authService;
    private final LookupService lookupService;
    private final RetryManager retryManager;
    private final HealthCheckService healthCheckService;
    private final ExecutorService ioExecutor;

    DanubeClient(
            URI serviceUri,
            ConnectionManager connectionManager,
            AuthService authService,
            LookupService lookupService,
            RetryManager retryManager,
            HealthCheckService healthCheckService,
            ExecutorService ioExecutor) {
        this.serviceUri = Objects.requireNonNull(serviceUri, "serviceUri");
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager");
        this.authService = Objects.requireNonNull(authService, "authService");
        this.lookupService = Objects.requireNonNull(lookupService, "lookupService");
        this.retryManager = Objects.requireNonNull(retryManager, "retryManager");
        this.healthCheckService = Objects.requireNonNull(healthCheckService, "healthCheckService");
        this.ioExecutor = Objects.requireNonNull(ioExecutor, "ioExecutor");
    }

    /** Returns a new builder for constructing a {@link DanubeClient}. */
    public static DanubeClientBuilder builder() {
        return new DanubeClientBuilder();
    }

    /** Returns the broker service URI this client is connected to. */
    public URI serviceUri() {
        return serviceUri;
    }

    /** Returns a new {@link ProducerBuilder} for creating a producer on this client. */
    public ProducerBuilder newProducer() {
        return new ProducerBuilder(this);
    }

    /** Returns a new {@link ConsumerBuilder} for creating a consumer on this client. */
    public ConsumerBuilder newConsumer() {
        return new ConsumerBuilder(this);
    }

    /** Returns a {@link SchemaRegistryClient} connected to this broker's schema registry. */
    public SchemaRegistryClient newSchemaRegistry() {
        return new SchemaRegistryClient(serviceUri, connectionManager, authService, ioExecutor);
    }

    /**
     * Resolves the broker address for a topic asynchronously.
     *
     * @param topic fully-qualified topic name, e.g. {@code /default/my-topic}
     * @return a future that resolves to the broker {@link LookupResult}
     */
    public CompletableFuture<LookupResult> lookupTopicAsync(String topic) {
        return CompletableFuture.supplyAsync(() -> lookupService.lookupTopic(serviceUri, topic), ioExecutor);
    }

    /**
     * Returns the partition topic names for a partitioned topic asynchronously.
     * Returns an empty list for non-partitioned topics.
     *
     * @param topic fully-qualified topic name
     * @return a future that resolves to the list of partition topic names
     */
    public CompletableFuture<List<String>> topicPartitionsAsync(String topic) {
        return CompletableFuture.supplyAsync(
                () -> lookupService.topicPartitions(serviceUri, topic),
                ioExecutor);
    }

    ConnectionManager connectionManager() {
        return connectionManager;
    }

    AuthService authService() {
        return authService;
    }

    LookupService lookupService() {
        return lookupService;
    }

    RetryManager retryManager() {
        return retryManager;
    }

    HealthCheckService healthCheckService() {
        return healthCheckService;
    }

    ExecutorService ioExecutor() {
        return ioExecutor;
    }

    @Override
    public void close() {
        ioExecutor.shutdown();
        connectionManager.close();
    }
}
