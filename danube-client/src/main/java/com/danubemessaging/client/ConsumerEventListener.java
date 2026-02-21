package com.danubemessaging.client;

import com.danubemessaging.client.model.StreamMessage;

/**
 * Consumer lifecycle, receive, and ack event hooks.
 */
public interface ConsumerEventListener {
    static ConsumerEventListener noop() {
        return new ConsumerEventListener() {
        };
    }

    default void onConsumerSubscribed(String topic, String consumerName, long consumerId) {
    }

    default void onConsumerClosed(String topic, String consumerName) {
    }

    default void onMessageReceived(String topic, String consumerName, StreamMessage message) {
    }

    default void onMessageAcked(String topic, String consumerName, StreamMessage message) {
    }

    default void onConsumerError(String topic, String consumerName, Throwable error, boolean retryable) {
    }

    default void onFatalReceiveError(String topic, String consumerName, Throwable error) {
        onFatalReceiveError(error);
    }

    default void onFatalReceiveError(Throwable error) {
    }
}
