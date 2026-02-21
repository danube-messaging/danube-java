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
    private int maxRetries;
    private long baseBackoffMs;
    private long maxBackoffMs;

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

    public ConsumerBuilder withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public ConsumerBuilder withBaseBackoffMs(long baseBackoffMs) {
        this.baseBackoffMs = baseBackoffMs;
        return this;
    }

    public ConsumerBuilder withMaxBackoffMs(long maxBackoffMs) {
        this.maxBackoffMs = maxBackoffMs;
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

        ConsumerOptions options = new ConsumerOptions(
                topic, consumerName, subscription, subType, eventListener,
                maxRetries, baseBackoffMs, maxBackoffMs);
        return new Consumer(client, options);
    }
}
