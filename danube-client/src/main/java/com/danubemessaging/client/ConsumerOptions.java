package com.danubemessaging.client;

/**
 * Immutable consumer configuration.
 */
public record ConsumerOptions(
        String topic,
        String consumerName,
        String subscription,
        SubType subType,
        ConsumerEventListener eventListener,
        int maxRetries,
        long baseBackoffMs,
        long maxBackoffMs) {
}
