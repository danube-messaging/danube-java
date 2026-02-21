package com.danubemessaging.client.schema;

import com.danubemessaging.client.errors.DanubeClientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Fluent builder for schema registration requests.
 */
public final class SchemaRegistrationBuilder {
    private String subject;
    private SchemaType schemaType;
    private byte[] schemaDefinition;
    private String description = "";
    private String createdBy = "";
    private final List<String> tags = new ArrayList<>();

    public SchemaRegistrationBuilder withSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public SchemaRegistrationBuilder withSchemaType(SchemaType schemaType) {
        this.schemaType = schemaType;
        return this;
    }

    public SchemaRegistrationBuilder withSchemaDefinition(byte[] schemaDefinition) {
        this.schemaDefinition = schemaDefinition == null ? null : schemaDefinition.clone();
        return this;
    }

    public SchemaRegistrationBuilder withDescription(String description) {
        this.description = description == null ? "" : description;
        return this;
    }

    public SchemaRegistrationBuilder withCreatedBy(String createdBy) {
        this.createdBy = createdBy == null ? "" : createdBy;
        return this;
    }

    public SchemaRegistrationBuilder addTag(String tag) {
        if (tag != null && !tag.isBlank()) {
            tags.add(tag);
        }
        return this;
    }

    public SchemaRegistrationBuilder addTags(Collection<String> values) {
        if (values != null) {
            values.forEach(this::addTag);
        }
        return this;
    }

    public SchemaRegistrationRequest build() {
        if (subject == null || subject.isBlank()) {
            throw new DanubeClientException("Schema subject is required");
        }
        if (schemaType == null) {
            throw new DanubeClientException("Schema type is required");
        }
        if (schemaDefinition == null || schemaDefinition.length == 0) {
            throw new DanubeClientException("Schema definition is required");
        }

        return new SchemaRegistrationRequest(
                subject,
                schemaType,
                schemaDefinition,
                description,
                createdBy,
                tags);
    }
}
