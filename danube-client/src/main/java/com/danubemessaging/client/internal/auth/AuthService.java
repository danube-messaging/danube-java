package com.danubemessaging.client.internal.auth;

import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.internal.connection.ConnectionManager;
import com.danubemessaging.client.internal.connection.ConnectionOptions;
import danube.AuthServiceGrpc;
import danube.DanubeApi;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handles API-key authentication and bearer token caching.
 */
public final class AuthService {
    private static final Duration TOKEN_EXPIRY = Duration.ofHours(1);
    private static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization",
            Metadata.ASCII_STRING_MARSHALLER);

    private final ConnectionManager connectionManager;
    private final ConnectionOptions connectionOptions;
    private final ReentrantLock tokenLock = new ReentrantLock();

    private volatile String token;
    private volatile Instant tokenExpiry;

    public AuthService(ConnectionManager connectionManager, ConnectionOptions connectionOptions) {
        this.connectionManager = connectionManager;
        this.connectionOptions = connectionOptions;
    }

    public String authenticateClient(URI address, String apiKey) {
        var grpcConnection = connectionManager.getConnection(address, address);
        var client = AuthServiceGrpc.newBlockingStub(grpcConnection.grpcChannel());
        var request = DanubeApi.AuthRequest.newBuilder().setApiKey(apiKey).build();

        try {
            var response = client.authenticate(request);
            cacheToken(response.getToken());
            return response.getToken();
        } catch (Exception e) {
            throw new DanubeClientException("Authentication failed", e);
        }
    }

    public String getValidToken(URI address, String apiKey) {
        String currentToken = token;
        Instant expiry = tokenExpiry;
        if (currentToken != null && expiry != null && Instant.now().isBefore(expiry)) {
            return currentToken;
        }

        tokenLock.lock();
        try {
            currentToken = token;
            expiry = tokenExpiry;
            if (currentToken != null && expiry != null && Instant.now().isBefore(expiry)) {
                return currentToken;
            }
            return authenticateClient(address, apiKey);
        } finally {
            tokenLock.unlock();
        }
    }

    public void insertTokenIfNeeded(Metadata metadata, URI address) {
        connectionOptions
                .apiKey()
                .ifPresent(apiKey -> metadata.put(AUTHORIZATION, "Bearer " + getValidToken(address, apiKey)));
    }

    public <T extends AbstractStub<T>> T attachAuthIfNeeded(T stub, URI address) {
        Metadata metadata = new Metadata();
        insertTokenIfNeeded(metadata, address);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    private void cacheToken(String tokenValue) {
        token = tokenValue;
        tokenExpiry = Instant.now().plus(TOKEN_EXPIRY);
    }
}
