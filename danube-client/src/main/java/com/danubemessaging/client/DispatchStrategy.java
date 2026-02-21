package com.danubemessaging.client;

import danube.DanubeApi;

/**
 * Delivery strategy used by producer instances.
 */
public enum DispatchStrategy {
    NON_RELIABLE,
    RELIABLE;

    public DanubeApi.DispatchStrategy toProto() {
        return switch (this) {
            case NON_RELIABLE -> DanubeApi.DispatchStrategy.NonReliable;
            case RELIABLE -> DanubeApi.DispatchStrategy.Reliable;
        };
    }
}
