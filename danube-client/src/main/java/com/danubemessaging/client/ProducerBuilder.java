package com.danubemessaging.client;

import com.danubemessaging.client.errors.DanubeClientException;

/**
 * Builder for {@link Producer}.
 */
public final class ProducerBuilder {
    private final DanubeClient client;

    private String topic;
    private String producerName;
    private ProducerAccessMode accessMode = ProducerAccessMode.SHARED;
    private DispatchStrategy dispatchStrategy = DispatchStrategy.NON_RELIABLE;
    private ProducerEventListener eventListener = ProducerEventListener.noop();

    ProducerBuilder(DanubeClient client) {
        this.client = client;
    }

    public ProducerBuilder withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public ProducerBuilder withName(String producerName) {
        this.producerName = producerName;
        return this;
    }

    public ProducerBuilder withAccessMode(ProducerAccessMode accessMode) {
        if (accessMode != null) {
            this.accessMode = accessMode;
        }
        return this;
    }

    public ProducerBuilder withDispatchStrategy(DispatchStrategy dispatchStrategy) {
        if (dispatchStrategy != null) {
            this.dispatchStrategy = dispatchStrategy;
        }
        return this;
    }

    public ProducerBuilder withEventListener(ProducerEventListener eventListener) {
        if (eventListener != null) {
            this.eventListener = eventListener;
        }
        return this;
    }

    public Producer build() {
        if (topic == null || topic.isBlank()) {
            throw new DanubeClientException("Producer topic is required");
        }

        if (producerName == null || producerName.isBlank()) {
            throw new DanubeClientException("Producer name is required");
        }

        ProducerOptions options = new ProducerOptions(topic, producerName, accessMode, dispatchStrategy, eventListener);
        return new Producer(client, options);
    }
}
