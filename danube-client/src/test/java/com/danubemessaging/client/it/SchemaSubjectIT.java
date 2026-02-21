package com.danubemessaging.client.it;

import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SchemaRegistryClient;
import com.danubemessaging.client.schema.SchemaInfo;
import com.danubemessaging.client.schema.SchemaType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.danubemessaging.client.it.TestHelpers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Schema registry integration tests: register a schema, create a producer
 * referencing it, send a message, and verify schema version evolution.
 */
class SchemaSubjectIT {

    @Test
    void producerWithRegisteredSchema() {
        DanubeClient client = newClient();
        String topic = uniqueTopic("/default/schema_registered");
        SchemaRegistryClient schemaClient = client.newSchemaRegistry();

        String jsonSchema = """
                {"type": "object", "properties": {"msg": {"type": "string"}}}""";

        schemaClient.registerSchema(
                schemaClient.newRegistration()
                        .withSubject("test-registered-schema-java")
                        .withSchemaType(SchemaType.JSON_SCHEMA)
                        .withSchemaDefinition(jsonSchema.getBytes()));

        Producer producer = client.newProducer()
                .withTopic(topic)
                .withName("producer_registered")
                .withSchemaLatest("test-registered-schema-java")
                .build();
        producer.create();

        producer.send("{\"msg\": \"hello\"}".getBytes(), Map.of());
        client.close();
    }

    @Test
    void producerWithoutSchema() {
        DanubeClient client = newClient();
        String topic = uniqueTopic("/default/no_schema");

        Producer producer = client.newProducer()
                .withTopic(topic)
                .withName("producer_no_schema")
                .build();
        producer.create();

        producer.send("any bytes work".getBytes(), Map.of());
        client.close();
    }

    @Test
    void schemaVersionEvolution() {
        DanubeClient client = newClient();
        SchemaRegistryClient schemaClient = client.newSchemaRegistry();

        String subject = uniqueTopic("versioned-schema-java");

        // V1
        String schemaV1 = """
                {"type": "object", "properties": {"name": {"type": "string"}}}""";
        var regV1 = schemaClient.registerSchema(
                schemaClient.newRegistration()
                        .withSubject(subject)
                        .withSchemaType(SchemaType.JSON_SCHEMA)
                        .withSchemaDefinition(schemaV1.getBytes()));

        // V2 (add optional field)
        String schemaV2 = """
                {"type": "object", "properties": {"name": {"type": "string"}, "email": {"type": "string"}}}""";
        var regV2 = schemaClient.registerSchema(
                schemaClient.newRegistration()
                        .withSubject(subject)
                        .withSchemaType(SchemaType.JSON_SCHEMA)
                        .withSchemaDefinition(schemaV2.getBytes()));

        // Same subject → same schema_id
        assertEquals(regV1.schemaId(), regV2.schemaId(),
                "expected same schema_id for same subject");

        // Latest should be V2
        SchemaInfo latest = schemaClient.getLatestSchema(subject);
        assertEquals(2, latest.version(), "expected latest version 2");

        client.close();
    }
}
