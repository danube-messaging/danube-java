package com.danubemessaging.client.model;

import java.net.URI;

/**
 * Result of a topic lookup operation.
 */
public record LookupResult(LookupType responseType, URI brokerUrl, URI connectUrl, boolean proxy) {
    public boolean isRedirect() {
        return responseType == LookupType.REDIRECT;
    }

    public boolean isConnect() {
        return responseType == LookupType.CONNECT;
    }
}
