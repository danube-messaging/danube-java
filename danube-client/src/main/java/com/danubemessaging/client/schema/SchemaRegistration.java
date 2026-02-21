package com.danubemessaging.client.schema;

/**
 * Result of schema registration operation.
 */
public record SchemaRegistration(long schemaId, int version, boolean newVersion, String fingerprint) {
}
