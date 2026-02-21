package com.danubemessaging.client.schema;

import com.danubemessaging.client.errors.DanubeClientException;
import java.util.Locale;

/**
 * Topic payload validation policy for schema-enabled topics.
 */
public enum ValidationPolicy {
    NONE("none"),
    WARN("warn"),
    ENFORCE("enforce");

    private final String wireValue;

    ValidationPolicy(String wireValue) {
        this.wireValue = wireValue;
    }

    public String toWireValue() {
        return wireValue;
    }

    public static ValidationPolicy fromWireValue(String wireValue) {
        if (wireValue == null || wireValue.isBlank()) {
            return NONE;
        }

        String normalized = wireValue.toLowerCase(Locale.ROOT);
        for (ValidationPolicy value : values()) {
            if (value.wireValue.equals(normalized)) {
                return value;
            }
        }

        throw new DanubeClientException("Unsupported validation policy: " + wireValue);
    }
}
