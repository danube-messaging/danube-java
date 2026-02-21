# danube-java

Java client library for Danube Messaging platform.

## Repository layout

- `danube-client-proto`: generated protobuf + gRPC Java stubs
- `danube-client`: handwritten Java client API and internals

## Where to copy proto files

Copy all `.proto` files here (preserve import paths):

`danube-client-proto/src/main/proto/`

Example:

```text
danube-client-proto/src/main/proto/
  DanubeApi.proto
  SchemaRegistry.proto
```

## How to generate Java stubs

No manual `protoc` install is required. Maven downloads the compiler/plugin.

From repo root:

```bash
mvn -pl danube-client-proto -am generate-sources
```

or via helper script:

```bash
bash scripts/generate-stubs.sh
```

Generated sources will be in:

- `danube-client-proto/target/generated-sources/protobuf/java`
- `danube-client-proto/target/generated-sources/protobuf/grpc-java`

## Build all modules

```bash
mvn clean verify
```

## Schema registry usage (Phase 5)

```java
DanubeClient client = DanubeClient.builder()
        .serviceUrl("http://localhost:6650")
        .build();

SchemaRegistryClient registry = client.newSchemaRegistry();

SchemaRegistration registration = registry.registerSchema(
        registry.newRegistration()
                .withSubject("/default/user-events")
                .withSchemaType(SchemaType.JSON_SCHEMA)
                .withSchemaDefinition(schemaBytes)
                .withDescription("User event payload")
                .withCreatedBy("java-client"));

Producer producer = client.newProducer()
        .withTopic("/default/user-events")
        .withName("orders-producer")
        .withSchemaLatest("/default/user-events")
        .build();
```

## Observability hooks (Phase 4)

```java
Producer producer = client.newProducer()
        .withTopic("/default/user-events")
        .withName("orders-producer")
        .withEventListener(new ProducerEventListener() {
            @Override
            public void onProducerCreated(String topic, String producerName, long producerId) {
                System.out.printf("Producer ready topic=%s id=%d%n", topic, producerId);
            }

            @Override
            public void onSendError(String topic, String producerName, byte[] payload, Throwable error) {
                System.err.printf("Send failed topic=%s cause=%s%n", topic, error.getMessage());
            }
        })
        .build();
```

## Consumer schema metadata

`StreamMessage` now includes optional schema registry metadata when available:

- `schemaId()`
- `schemaVersion()`

```java
consumer.receiveAsync().thenAccept(msg -> {
    if (msg.schemaId() != null) {
        System.out.printf("Schema id=%d version=%s%n", msg.schemaId(), msg.schemaVersion());
    }
});
```

## Examples

Standalone runnable examples are available in:

- [`examples/`](./examples)

## Release preparation

For Maven Central publishing and signing workflow, see:

- [`RELEASE.md`](./RELEASE.md)
