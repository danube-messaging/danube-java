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

    /**
     * Sets the fully-qualified topic name. Required.
     *
     * @param topic e.g. {@code /default/my-topic}
     */
    public ConsumerBuilder withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * Sets the consumer name. Required. Used to identify this consumer on the broker.
     *
     * @param consumerName a unique identifier for this consumer
     */
    public ConsumerBuilder withConsumerName(String consumerName) {
        this.consumerName = consumerName;
        return this;
    }

    /**
     * Sets the subscription name. Required.
     * Multiple consumers sharing the same subscription name form a subscription group.
     *
     * @param subscription the subscription name
     */
    public ConsumerBuilder withSubscription(String subscription) {
        this.subscription = subscription;
        return this;
    }

    /**
     * Sets the subscription type. Defaults to {@link SubType#SHARED}.
     *
     * @param subType {@link SubType#EXCLUSIVE}, {@link SubType#SHARED}, or {@link SubType#FAILOVER}
     */
    public ConsumerBuilder withSubscriptionType(SubType subType) {
        if (subType != null) {
            this.subType = subType;
        }
        return this;
    }

    /**
     * Sets a listener for consumer lifecycle and message events.
     *
     * @param eventListener the listener; no-op by default
     */
    public ConsumerBuilder withEventListener(ConsumerEventListener eventListener) {
        if (eventListener != null) {
            this.eventListener = eventListener;
        }
        return this;
    }

    /**
     * Sets the maximum number of receive retries before giving up.
     *
     * @param maxRetries number of retries; 0 means use the client default
     */
    public ConsumerBuilder withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Sets the base backoff duration between retries (milliseconds).
     *
     * @param baseBackoffMs base backoff in ms; 0 means use the client default
     */
    public ConsumerBuilder withBaseBackoffMs(long baseBackoffMs) {
        this.baseBackoffMs = baseBackoffMs;
        return this;
    }

    /**
     * Sets the maximum backoff duration between retries (milliseconds).
     *
     * @param maxBackoffMs max backoff in ms; 0 means use the client default
     */
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
