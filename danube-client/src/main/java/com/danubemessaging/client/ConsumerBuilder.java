package com.danubemessaging.client;

import com.danubemessaging.client.errors.DanubeClientException;

/**
 * Builder for {@link Consumer}.
 */
public final class ConsumerBuilder {
    private final DanubeClient client;

    private String topic;
    private String consumerName;
    private String subscription;
    private SubType subType = SubType.SHARED;
    private ConsumerEventListener eventListener = ConsumerEventListener.noop();

    ConsumerBuilder(DanubeClient client) {
        this.client = client;
    }

    public ConsumerBuilder withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public ConsumerBuilder withConsumerName(String consumerName) {
        this.consumerName = consumerName;
        return this;
    }

    public ConsumerBuilder withSubscription(String subscription) {
        this.subscription = subscription;
        return this;
    }

    public ConsumerBuilder withSubscriptionType(SubType subType) {
        if (subType != null) {
            this.subType = subType;
        }
        return this;
    }

    public ConsumerBuilder withEventListener(ConsumerEventListener eventListener) {
        if (eventListener != null) {
            this.eventListener = eventListener;
        }
        return this;
    }

    public Consumer build() {
        if (topic == null || topic.isBlank()) {
            throw new DanubeClientException("Consumer topic is required");
        }

        if (consumerName == null || consumerName.isBlank()) {
            throw new DanubeClientException("Consumer name is required");
        }

        if (subscription == null || subscription.isBlank()) {
            throw new DanubeClientException("Subscription is required");
        }

        ConsumerOptions options = new ConsumerOptions(topic, consumerName, subscription, subType, eventListener);
        return new Consumer(client, options);
    }
}
