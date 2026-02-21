package com.danubemessaging.client;

import com.danubemessaging.client.schema.SchemaReference;

/**
 * Immutable producer configuration.
 */
public record ProducerOptions(
        String topic,
        String producerName,
        ProducerAccessMode accessMode,
        DispatchStrategy dispatchStrategy,
        SchemaReference schemaReference,
        ProducerEventListener eventListener) {
}
