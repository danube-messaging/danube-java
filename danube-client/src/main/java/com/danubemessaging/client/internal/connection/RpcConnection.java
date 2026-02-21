package com.danubemessaging.client.internal.connection;

import io.grpc.ManagedChannel;

/**
 * Wrapper for a broker gRPC channel.
 */
public record RpcConnection(ManagedChannel grpcChannel) {}
