package com.danubemessaging.client.errors;

/**
 * Base runtime exception used by the Danube Java client.
 */
public class DanubeClientException extends RuntimeException {
    public DanubeClientException(String message) {
        super(message);
    }

    public DanubeClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public DanubeClientException(Throwable cause) {
        super(cause);
    }
}
