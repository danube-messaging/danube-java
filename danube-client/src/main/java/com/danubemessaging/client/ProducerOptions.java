package com.danubemessaging.client;

/**
 * Immutable producer configuration.
 */
public record ProducerOptions(
                String topic,
                String producerName,
                ProducerAccessMode accessMode,
                DispatchStrategy dispatchStrategy,
                ProducerEventListener eventListener) {
}
