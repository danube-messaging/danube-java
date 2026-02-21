package com.danubemessaging.client.schema;

/**
 * Generic success/message response used by schema registry operations.
 */
public record OperationResult(boolean success, String message) {
}
