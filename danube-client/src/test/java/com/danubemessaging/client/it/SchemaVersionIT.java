package com.danubemessaging.client.it;

import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SchemaRegistryClient;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;
import com.danubemessaging.client.schema.SchemaType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.danubemessaging.client.it.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Schema version pinning and latest-version resolution tests.
 */
class SchemaVersionIT {

    @Test
    void producerPinToVersion() throws Exception {
        DanubeClient client = newClient();
        String topic = uniqueTopic("/default/pin_version");
        SchemaRegistryClient schemaClient = client.newSchemaRegistry();

        String subject = uniqueTopic("version-pin-java");

        // Register V1
        String schemaV1 = """
                {"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}""";
        schemaClient.registerSchema(
                schemaClient.newRegistration()
                        .withSubject(subject)
                        .withSchemaType(SchemaType.JSON_SCHEMA)
                        .withSchemaDefinition(schemaV1.getBytes()));

        // Register V2
        String schemaV2 = """
                {"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id"]}""";
        schemaClient.registerSchema(
                schemaClient.newRegistration()
                        .withSubject(subject)
                        .withSchemaType(SchemaType.JSON_SCHEMA)
                        .withSchemaDefinition(schemaV2.getBytes()));

        // Producer pinned to V1
        Producer producer = client.newProducer()
                .withTopic(topic)
                .withName("producer_v1_pinned")
                .withSchemaPinnedVersion(subject, 1)
                .build();
        producer.create();

        // Consumer
        Consumer consumer = client.newConsumer()
                .withTopic(topic)
                .withConsumerName("consumer_version")
                .withSubscription("sub_version")
                .withSubscriptionType(SubType.EXCLUSIVE)
                .build();
        consumer.subscribe();

        try {
            var publisher = consumer.receive();
            Thread.sleep(200);

            producer.send("{\"id\": 123}".getBytes(), Map.of());

            StreamMessage msg = receiveOne(publisher, Duration.ofSeconds(5));

            assertNotNull(msg.schemaVersion(), "expected schema_version to be set");
            assertEquals(1, msg.schemaVersion(), "expected schema_version=1");

            consumer.ack(msg);
        } finally {
            consumer.close();
            client.close();
        }
    }

    @Test
    void producerLatestVersion() throws Exception {
        DanubeClient client = newClient();
        String topic = uniqueTopic("/default/latest_version");
        SchemaRegistryClient schemaClient = client.newSchemaRegistry();

        String subject = uniqueTopic("latest-version-java");

        // Register V1
        String schemaV1 = """
                {"type": "object", "properties": {"a": {"type": "string"}}}""";
        schemaClient.registerSchema(
                schemaClient.newRegistration()
                        .withSubject(subject)
                        .withSchemaType(SchemaType.JSON_SCHEMA)
                        .withSchemaDefinition(schemaV1.getBytes()));

        // Register V2
        String schemaV2 = """
                {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}}""";
        schemaClient.registerSchema(
                schemaClient.newRegistration()
                        .withSubject(subject)
                        .withSchemaType(SchemaType.JSON_SCHEMA)
                        .withSchemaDefinition(schemaV2.getBytes()));

        // Producer without version pin (should use latest V2)
        Producer producer = client.newProducer()
                .withTopic(topic)
                .withName("producer_latest")
                .withSchemaLatest(subject)
                .build();
        producer.create();

        // Consumer
        Consumer consumer = client.newConsumer()
                .withTopic(topic)
                .withConsumerName("consumer_latest")
                .withSubscription("sub_latest")
                .build();
        consumer.subscribe();

        try {
            var publisher = consumer.receive();
            Thread.sleep(200);

            producer.send("{\"a\": \"test\"}".getBytes(), Map.of());

            StreamMessage msg = receiveOne(publisher, Duration.ofSeconds(5));

            assertNotNull(msg.schemaVersion(), "expected schema_version to be set");
            assertEquals(2, msg.schemaVersion(), "expected schema_version=2 (latest)");

            consumer.ack(msg);
        } finally {
            consumer.close();
            client.close();
        }
    }
}
