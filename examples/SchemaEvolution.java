import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.SchemaRegistryClient;
import com.danubemessaging.client.schema.CompatibilityCheck;
import com.danubemessaging.client.schema.SchemaType;
import com.danubemessaging.client.schema.SchemaVersionInfo;

import java.util.List;

/**
 * Schema evolution example: demonstrates registering, evolving, and
 * compatibility-checking schemas in the Danube schema registry.
 *
 * Key concepts:
 *  - BACKWARD compatibility (default): new schema can read data written by old schema.
 *  - Adding an optional field (with default) is compatible.
 *  - Adding a required field without default is NOT compatible.
 *
 * Prerequisites: Danube broker running on localhost:6650
 *   cd docker && docker compose up -d
 */
public class SchemaEvolution {

    private static final String BROKER_URL = System.getenv().getOrDefault("DANUBE_BROKER_URL", "http://127.0.0.1:6650");
    private static final String SUBJECT = "product-catalog";

    private static final String SCHEMA_V1 = """
            {
              "type": "record",
              "name": "Product",
              "namespace": "com.example.catalog",
              "fields": [
                {"name": "product_id", "type": "string"},
                {"name": "name",       "type": "string"},
                {"name": "price",      "type": "double"}
              ]
            }""";

    private static final String SCHEMA_V2 = """
            {
              "type": "record",
              "name": "Product",
              "namespace": "com.example.catalog",
              "fields": [
                {"name": "product_id",  "type": "string"},
                {"name": "name",        "type": "string"},
                {"name": "price",       "type": "double"},
                {"name": "description", "type": ["null", "string"], "default": null}
              ]
            }""";

    private static final String SCHEMA_V3_INCOMPATIBLE = """
            {
              "type": "record",
              "name": "Product",
              "namespace": "com.example.catalog",
              "fields": [
                {"name": "product_id",  "type": "string"},
                {"name": "name",        "type": "string"},
                {"name": "price",       "type": "double"},
                {"name": "description", "type": ["null", "string"], "default": null},
                {"name": "category",    "type": "string"}
              ]
            }""";

    public static void main(String[] args) throws Exception {
        DanubeClient client = DanubeClient.builder()
                .serviceUrl(BROKER_URL)
                .build();

        SchemaRegistryClient schemaClient = client.newSchemaRegistry();

        // Step 1: Register the initial schema (v1)
        System.out.println("Step 1: Registering initial schema (v1)");

        var regV1 = schemaClient.registerSchema(
                schemaClient.newRegistration()
                        .withSubject(SUBJECT)
                        .withSchemaType(SchemaType.AVRO)
                        .withSchemaDefinition(SCHEMA_V1.getBytes()));

        System.out.printf("  Schema v1 registered — id=%d version=%d%n",
                regV1.schemaId(), regV1.version());

        // Give the broker a moment to sync metadata to local cache
        System.out.println("  Waiting for metadata to sync...");
        Thread.sleep(1500);

        // Step 2: Check compatibility for schema v2 (adds optional field — should be OK)
        System.out.println("\nStep 2: Checking compatibility for schema v2 (adds optional field)");

        CompatibilityCheck checkV2 = schemaClient.checkCompatibility(
                SUBJECT, SCHEMA_V2.getBytes(), SchemaType.AVRO);

        if (checkV2.compatible()) {
            System.out.println("  Schema v2 is COMPATIBLE — safe to register");

            var regV2 = schemaClient.registerSchema(
                    schemaClient.newRegistration()
                            .withSubject(SUBJECT)
                            .withSchemaType(SchemaType.AVRO)
                            .withSchemaDefinition(SCHEMA_V2.getBytes()));

            System.out.printf("  Schema v2 registered — id=%d version=%d%n",
                    regV2.schemaId(), regV2.version());
        } else {
            System.out.println("  Schema v2 is NOT compatible (unexpected)");
            System.out.println("  Errors: " + checkV2.errors());
        }

        // Step 3: Try to register an incompatible schema (adds required field — should fail)
        System.out.println("\nStep 3: Testing incompatible schema v3 (adds required field without default)");

        CompatibilityCheck checkV3 = schemaClient.checkCompatibility(
                SUBJECT, SCHEMA_V3_INCOMPATIBLE.getBytes(), SchemaType.AVRO);

        if (checkV3.compatible()) {
            System.out.println("  Schema v3 is compatible (unexpected — required field should break old data)");
        } else {
            System.out.println("  Schema v3 is NOT compatible (expected)");
            System.out.println("  Reason: added required field 'category' without default");
            System.out.println("  This protects against breaking consumers reading old data");
            if (!checkV3.errors().isEmpty()) {
                System.out.println("  Errors: " + checkV3.errors());
            }
        }

        // Step 4: List all registered versions
        System.out.println("\nStep 4: Listing all schema versions");

        List<SchemaVersionInfo> versions = schemaClient.listVersions(SUBJECT);
        System.out.printf("  Schema versions for '%s': %d version(s)%n", SUBJECT, versions.size());
        for (SchemaVersionInfo v : versions) {
            System.out.printf("    version=%d fingerprint=%s%n", v.version(), v.fingerprint());
        }

        // Step 5: Retrieve the latest schema
        System.out.println("\nStep 5: Retrieving latest schema");

        var latest = schemaClient.getLatestSchema(SUBJECT);
        System.out.printf("  Subject: %s%n", latest.subject());
        System.out.printf("  Version: %d%n", latest.version());
        System.out.printf("  Type:    %s%n", latest.schemaType());

        System.out.println("\nSchema evolution demo completed!");
        System.out.println("  Key takeaways:");
        System.out.println("  - Adding optional fields (with default): COMPATIBLE");
        System.out.println("  - Adding required fields without default: INCOMPATIBLE");
        System.out.println("  - Compatibility mode: BACKWARD (new schema reads old data)");

        client.close();
    }
}
