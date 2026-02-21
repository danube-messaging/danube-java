package com.danubemessaging.client;

/**
 * Producer lifecycle and send event hooks.
 */
public interface ProducerEventListener {
    static ProducerEventListener noop() {
        return new ProducerEventListener() {
        };
    }

    default void onProducerCreated(String topic, String producerName, long producerId) {
    }

    default void onProducerClosed(String topic, String producerName) {
    }

    default void onMessageSent(
            String topic,
            String producerName,
            long requestId,
            int payloadBytes,
            int attributeCount) {
    }

    default void onProducerError(String topic, String producerName, Throwable error, boolean retryable) {
    }
}
