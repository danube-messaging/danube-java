package com.danubemessaging.client.schema;

/**
 * Schema version metadata for a subject.
 */
public record SchemaVersionInfo(
        int version,
        long createdAt,
        String createdBy,
        String description,
        String fingerprint,
        long schemaId) {
}
