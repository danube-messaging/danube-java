package com.danubemessaging.client.schema;

import java.util.List;

/**
 * Schema compatibility check result.
 */
public record CompatibilityCheck(boolean compatible, List<String> errors) {
    public CompatibilityCheck {
        errors = errors == null ? List.of() : List.copyOf(errors);
    }
}
