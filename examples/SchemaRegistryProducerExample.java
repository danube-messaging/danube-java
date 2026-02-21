import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SchemaRegistryClient;
import com.danubemessaging.client.schema.SchemaType;
import java.nio.charset.StandardCharsets;

/**
 * Example: register a schema and create a producer bound to the latest schema version.
 */
public final class SchemaRegistryProducerExample {
    private SchemaRegistryProducerExample() {
    }

    public static void main(String[] args) {
        DanubeClient client = DanubeClient.builder()
                .serviceUrl("http://localhost:6650")
                .build();

        try {
            SchemaRegistryClient registry = client.newSchemaRegistry();
            byte[] schemaBytes = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"]}"
                    .getBytes(StandardCharsets.UTF_8);

            registry.registerSchema(
                    registry.newRegistration()
                            .withSubject("/default/user-events")
                            .withSchemaType(SchemaType.JSON_SCHEMA)
                            .withSchemaDefinition(schemaBytes)
                            .withDescription("Example user event schema")
                            .withCreatedBy("schema-registry-producer-example")
                            .addTag("example")
                            .addTag("phase-5"));

            Producer producer = client.newProducer()
                    .withTopic("/default/user-events")
                    .withName("example-producer")
                    .withSchemaLatest("/default/user-events")
                    .build();

            producer.create();
            producer.send("{\"id\":\"u-100\"}".getBytes(StandardCharsets.UTF_8), null);
            producer.close();
        } finally {
            client.close();
        }
    }
}
