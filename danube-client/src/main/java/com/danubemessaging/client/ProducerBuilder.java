package com.danubemessaging.client;

import com.danubemessaging.client.errors.DanubeClientException;
import com.danubemessaging.client.schema.SchemaReference;

/**
 * Builder for {@link Producer}.
 */
public final class ProducerBuilder {
    private final DanubeClient client;

    private String topic;
    private String producerName;
    private ProducerAccessMode accessMode = ProducerAccessMode.SHARED;
    private DispatchStrategy dispatchStrategy = DispatchStrategy.NON_RELIABLE;
    private SchemaReference schemaReference;
    private ProducerEventListener eventListener = ProducerEventListener.noop();
    private int maxRetries;
    private long baseBackoffMs;
    private long maxBackoffMs;
    private int partitions;

    ProducerBuilder(DanubeClient client) {
        this.client = client;
    }

    /**
     * Sets the fully-qualified topic name. Required.
     *
     * @param topic e.g. {@code /default/my-topic}
     */
    public ProducerBuilder withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * Sets the producer name. Required. Must be unique per topic on the broker.
     *
     * @param producerName a unique identifier for this producer
     */
    public ProducerBuilder withName(String producerName) {
        this.producerName = producerName;
        return this;
    }

    /**
     * Sets the producer access mode. Defaults to {@link ProducerAccessMode#SHARED}.
     *
     * @param accessMode the access mode for concurrent producers on the same topic
     */
    public ProducerBuilder withAccessMode(ProducerAccessMode accessMode) {
        if (accessMode != null) {
            this.accessMode = accessMode;
        }
        return this;
    }

    /**
     * Sets the message dispatch strategy. Defaults to {@link DispatchStrategy#NON_RELIABLE}.
     * Use {@link DispatchStrategy#RELIABLE} for guaranteed delivery with WAL persistence.
     *
     * @param dispatchStrategy the dispatch strategy
     */
    public ProducerBuilder withDispatchStrategy(DispatchStrategy dispatchStrategy) {
        if (dispatchStrategy != null) {
            this.dispatchStrategy = dispatchStrategy;
        }
        return this;
    }

    /**
     * Sets a custom {@link SchemaReference} for schema-tagged messages.
     *
     * @param schemaReference the schema reference
     */
    public ProducerBuilder withSchemaReference(SchemaReference schemaReference) {
        this.schemaReference = schemaReference;
        return this;
    }

    /**
     * Attaches the latest registered schema for the given subject.
     * The producer will resolve the current schema ID/version at creation time.
     *
     * @param subject the schema subject name
     */
    public ProducerBuilder withSchemaLatest(String subject) {
        this.schemaReference = SchemaReference.latest(subject);
        return this;
    }

    /**
     * Attaches a specific schema version for the given subject.
     *
     * @param subject the schema subject name
     * @param version the exact schema version to pin
     */
    public ProducerBuilder withSchemaPinnedVersion(String subject, int version) {
        this.schemaReference = SchemaReference.pinnedVersion(subject, version);
        return this;
    }

    /**
     * Requires the schema version to be at least {@code minVersion}.
     *
     * @param subject    the schema subject name
     * @param minVersion the minimum acceptable schema version
     */
    public ProducerBuilder withSchemaMinVersion(String subject, int minVersion) {
        this.schemaReference = SchemaReference.minVersion(subject, minVersion);
        return this;
    }

    /**
     * Sets a listener for producer lifecycle and error events.
     *
     * @param eventListener the listener; no-op by default
     */
    public ProducerBuilder withEventListener(ProducerEventListener eventListener) {
        if (eventListener != null) {
            this.eventListener = eventListener;
        }
        return this;
    }

    /**
     * Sets the maximum number of send retries before giving up.
     *
     * @param maxRetries number of retries; 0 means use the client default
     */
    public ProducerBuilder withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Sets the base backoff duration between retries (milliseconds).
     *
     * @param baseBackoffMs base backoff in ms; 0 means use the client default
     */
    public ProducerBuilder withBaseBackoffMs(long baseBackoffMs) {
        this.baseBackoffMs = baseBackoffMs;
        return this;
    }

    /**
     * Sets the maximum backoff duration between retries (milliseconds).
     *
     * @param maxBackoffMs max backoff in ms; 0 means use the client default
     */
    public ProducerBuilder withMaxBackoffMs(long maxBackoffMs) {
        this.maxBackoffMs = maxBackoffMs;
        return this;
    }

    /**
     * Declares a fixed number of partitions for this producer.
     * When set, partition topics are named {@code {topic}-part-{i}} (0-indexed)
     * and partition lookup is skipped. Omit to use auto-discovery.
     *
     * @param partitions number of partitions; must be &gt; 0
     */
    public ProducerBuilder withPartitions(int partitions) {
        if (partitions < 0) {
            throw new DanubeClientException("Partitions must be zero or positive");
        }
        this.partitions = partitions;
        return this;
    }

    public Producer build() {
        if (topic == null || topic.isBlank()) {
            throw new DanubeClientException("Producer topic is required");
        }

        if (producerName == null || producerName.isBlank()) {
            throw new DanubeClientException("Producer name is required");
        }

        ProducerOptions options = new ProducerOptions(
                topic,
                producerName,
                accessMode,
                dispatchStrategy,
                schemaReference,
                eventListener,
                maxRetries,
                baseBackoffMs,
                maxBackoffMs,
                partitions);
        return new Producer(client, options);
    }
}
