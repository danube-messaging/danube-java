package com.danubemessaging.client.schema;

import com.danubemessaging.client.errors.DanubeClientException;
import java.util.Locale;

/**
 * Compatibility policy used by schema registry subjects.
 */
public enum CompatibilityMode {
    NONE("none"),
    BACKWARD("backward"),
    FORWARD("forward"),
    FULL("full");

    private final String wireValue;

    CompatibilityMode(String wireValue) {
        this.wireValue = wireValue;
    }

    public String toWireValue() {
        return wireValue;
    }

    public static CompatibilityMode fromWireValue(String wireValue) {
        if (wireValue == null || wireValue.isBlank()) {
            return NONE;
        }

        String normalized = wireValue.toLowerCase(Locale.ROOT);
        for (CompatibilityMode value : values()) {
            if (value.wireValue.equals(normalized)) {
                return value;
            }
        }

        throw new DanubeClientException("Unsupported compatibility mode: " + wireValue);
    }
}
