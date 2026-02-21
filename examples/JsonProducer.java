import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SchemaRegistryClient;
import com.danubemessaging.client.schema.SchemaType;

import java.util.Map;

/**
 * JSON producer example: registers a JSON schema in the schema registry and
 * produces schema-tagged messages to a topic.
 *
 * Run the JSON consumer (JsonConsumer.java) in a separate terminal first.
 *
 * Prerequisites: Danube broker running on localhost:6650
 *   cd docker && docker compose up -d
 */
public class JsonProducer {

    private static final String BROKER_URL = System.getenv().getOrDefault("DANUBE_BROKER_URL", "http://127.0.0.1:6650");
    private static final String TOPIC = "/default/json_topic";
    private static final String SUBJECT = "my-app-events";

    private static final String JSON_SCHEMA = """
            {
              "type": "object",
              "properties": {
                "field1": {"type": "string"},
                "field2": {"type": "integer"}
              }
            }""";

    public static void main(String[] args) throws Exception {
        DanubeClient client = DanubeClient.builder()
                .serviceUrl(BROKER_URL)
                .build();

        // Register schema in schema registry
        SchemaRegistryClient schemaClient = client.newSchemaRegistry();

        var registration = schemaClient.registerSchema(
                schemaClient.newRegistration()
                        .withSubject(SUBJECT)
                        .withSchemaType(SchemaType.JSON_SCHEMA)
                        .withSchemaDefinition(JSON_SCHEMA.getBytes()));

        System.out.printf("Registered schema '%s' — id=%d version=%d%n",
                SUBJECT, registration.schemaId(), registration.version());

        // Create producer pinned to the latest schema for this subject
        Producer producer = client.newProducer()
                .withTopic(TOPIC)
                .withName("prod_json")
                .withSchemaLatest(SUBJECT)
                .build();

        producer.create();
        System.out.println("Producer created");

        for (int i = 0; i < 10; i++) {
            String json = String.format("{\"field1\": \"value%d\", \"field2\": %d}", i, 2020 + i);
            long msgId = producer.send(json.getBytes(), Map.of());
            System.out.printf("Sent message #%d (id=%d): %s%n", i, msgId, json);
            Thread.sleep(1000);
        }

        client.close();
    }
}
