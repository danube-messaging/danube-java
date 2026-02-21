package com.danubemessaging.client.model;

/**
 * Topic lookup response type returned by Danube brokers.
 */
public enum LookupType {
    REDIRECT,
    CONNECT,
    FAILED,
    UNKNOWN;

    public static LookupType fromProtoNumber(int number) {
        return switch (number) {
            case 0 -> REDIRECT;
            case 1 -> CONNECT;
            case 2 -> FAILED;
            default -> UNKNOWN;
        };
    }
}
