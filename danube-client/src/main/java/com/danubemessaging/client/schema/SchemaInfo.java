package com.danubemessaging.client.schema;

import java.util.List;

/**
 * Full schema definition metadata returned by schema registry.
 */
public record SchemaInfo(
        long schemaId,
        int version,
        String subject,
        SchemaType schemaType,
        byte[] schemaDefinition,
        String description,
        long createdAt,
        String createdBy,
        List<String> tags,
        String fingerprint,
        CompatibilityMode compatibilityMode) {

    public SchemaInfo {
        schemaDefinition = schemaDefinition == null ? new byte[0] : schemaDefinition.clone();
        tags = tags == null ? List.of() : List.copyOf(tags);
    }

    @Override
    public byte[] schemaDefinition() {
        return schemaDefinition.clone();
    }
}
