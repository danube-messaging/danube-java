package com.danubemessaging.client;

import danube.DanubeApi;

/**
 * Access mode when creating a producer on a topic.
 */
public enum ProducerAccessMode {
    SHARED,
    EXCLUSIVE;

    public DanubeApi.ProducerAccessMode toProto() {
        return switch (this) {
            case SHARED -> DanubeApi.ProducerAccessMode.Shared;
            case EXCLUSIVE -> DanubeApi.ProducerAccessMode.Exclusive;
        };
    }
}
