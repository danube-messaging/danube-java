package com.danubemessaging.client;

import danube.DanubeApi;

/**
 * Consumer subscription type.
 */
public enum SubType {
    EXCLUSIVE,
    SHARED,
    FAILOVER;

    public DanubeApi.ConsumerRequest.SubscriptionType toProto() {
        return switch (this) {
            case EXCLUSIVE -> DanubeApi.ConsumerRequest.SubscriptionType.Exclusive;
            case SHARED -> DanubeApi.ConsumerRequest.SubscriptionType.Shared;
            case FAILOVER -> DanubeApi.ConsumerRequest.SubscriptionType.Failover;
        };
    }
}
