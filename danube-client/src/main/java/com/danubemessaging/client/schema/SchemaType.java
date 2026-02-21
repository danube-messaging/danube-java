package com.danubemessaging.client.schema;

import com.danubemessaging.client.errors.DanubeClientException;
import java.util.Locale;

/**
 * Supported schema types in Danube schema registry.
 */
public enum SchemaType {
    AVRO("avro"),
    JSON_SCHEMA("json_schema"),
    PROTOBUF("protobuf"),
    BYTES("bytes"),
    STRING("string"),
    NUMBER("number");

    private final String wireValue;

    SchemaType(String wireValue) {
        this.wireValue = wireValue;
    }

    public String toWireValue() {
        return wireValue;
    }

    public static SchemaType fromWireValue(String wireValue) {
        if (wireValue == null || wireValue.isBlank()) {
            throw new DanubeClientException("Schema type is required");
        }

        String normalized = wireValue.toLowerCase(Locale.ROOT);
        for (SchemaType value : values()) {
            if (value.wireValue.equals(normalized)) {
                return value;
            }
        }

        throw new DanubeClientException("Unsupported schema type: " + wireValue);
    }
}
