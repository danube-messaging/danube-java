package com.danubemessaging.client.schema;

/**
 * Topic-level schema validation configuration.
 */
public record TopicSchemaConfig(
        String schemaSubject,
        ValidationPolicy validationPolicy,
        boolean payloadValidationEnabled,
        long schemaId) {
}
