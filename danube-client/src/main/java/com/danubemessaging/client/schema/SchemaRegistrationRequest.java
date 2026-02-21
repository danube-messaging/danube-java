package com.danubemessaging.client.schema;

import com.danubemessaging.client.errors.DanubeClientException;
import java.util.List;

/**
 * Input payload for schema registration.
 */
public record SchemaRegistrationRequest(
        String subject,
        SchemaType schemaType,
        byte[] schemaDefinition,
        String description,
        String createdBy,
        List<String> tags) {

    public SchemaRegistrationRequest {
        if (subject == null || subject.isBlank()) {
            throw new DanubeClientException("Schema subject is required");
        }
        if (schemaType == null) {
            throw new DanubeClientException("Schema type is required");
        }

        schemaDefinition = schemaDefinition == null ? new byte[0] : schemaDefinition.clone();
        if (schemaDefinition.length == 0) {
            throw new DanubeClientException("Schema definition is required");
        }

        description = description == null ? "" : description;
        createdBy = createdBy == null ? "" : createdBy;
        tags = tags == null ? List.of() : List.copyOf(tags);
    }

    @Override
    public byte[] schemaDefinition() {
        return schemaDefinition.clone();
    }
}
