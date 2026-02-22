# Danube-java client

The Java client library for interacting with Danube Messaging Broker platform.

[Danube](https://github.com/danube-messaging/danube) is an open-source **distributed** Messaging platform written in Rust. Consult [the documentation](https://danube-docs.dev-state.com/) for supported concepts and the platform architecture.

## Features

### 📤 Producer Capabilities

- **Basic Messaging** - Send messages with byte payloads and optional key-value attributes
- **Partitioned Topics** - Distribute messages across multiple partitions for horizontal scaling
- **Reliable Dispatch** - Guaranteed message delivery with persistence (WAL + cloud storage)
- **Schema Integration** - Type-safe messaging with automatic validation (Bytes, String, Number, Avro, JSON Schema, Protobuf)

### 📥 Consumer Capabilities

- **Flexible Subscriptions** - Three subscription types for different use cases:
  - **Exclusive** - Single active consumer, guaranteed ordering
  - **Shared** - Load balancing across multiple consumers, parallel processing
  - **Failover** - High availability with automatic standby promotion
- **Message Acknowledgment** - Reliable message processing with at-least-once delivery
- **Partitioned Consumption** - Automatic handling of messages from all partitions
- **Message Attributes** - Access metadata and custom headers

### 🔐 Schema Registry

- **Schema Management** - Register, version, and retrieve schemas (JSON Schema, Avro, Protobuf)
- **Compatibility Checking** - Validate schema evolution (Backward, Forward, Full, None modes)
- **Type Safety** - Automatic validation against registered schemas
- **Schema Evolution** - Safe schema updates with compatibility enforcement

### 🏗️ Client Features

- **Virtual Threads** - Built on Project Loom for efficient I/O without blocking platform threads
- **Reactive API** - `Flow.Publisher<StreamMessage>` receive API (Java standard)
- **Connection Pooling** - Shared gRPC channel management across producers/consumers
- **TLS / mTLS** - Secure connections with custom CA and client certificates
- **JWT Authentication** - API-key based token exchange with automatic renewal
- **Topic Namespaces** - Organize topics with namespace structure (`/namespace/topic-name`)

## Installation

### Maven

```xml
<dependency>
    <groupId>com.danube-messaging</groupId>
    <artifactId>danube-client</artifactId>
    <version>0.2.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.danube-messaging:danube-client:0.2.0'
```

**Requirements:** Java 21 or later.

## Example Usage

Check out the [example files](https://github.com/danube-messaging/danube-java/tree/main/examples).

### Start the Danube server

Use the [instructions from the documentation](https://danube-docs.dev-state.com/) to run the Danube broker/cluster.

### Create Producer

```java
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import java.util.Map;

DanubeClient client = DanubeClient.builder()
        .serviceUrl("http://127.0.0.1:6650")
        .build();

String topic = "/default/test_topic";
String producerName = "test_producer";

Producer producer = client.newProducer()
        .withTopic(topic)
        .withName(producerName)
        .build();

producer.create();
System.out.printf("The Producer %s was created%n", producerName);

byte[] payload = "Hello Danube".getBytes();
long messageId = producer.send(payload, Map.of());
System.out.printf("The Message with id %d was sent%n", messageId);

client.close();
```

### Reliable Dispatch (optional)

Reliable dispatch can be enabled when creating the producer; the broker will stream messages to consumers from WAL and cloud storage.

```java
import com.danubemessaging.client.DispatchStrategy;

Producer producer = client.newProducer()
        .withTopic(topic)
        .withName(producerName)
        .withDispatchStrategy(DispatchStrategy.RELIABLE)
        .build();
```

### Create Consumer

```java
import com.danubemessaging.client.Consumer;
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.SubType;
import com.danubemessaging.client.model.StreamMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

DanubeClient client = DanubeClient.builder()
        .serviceUrl("http://127.0.0.1:6650")
        .build();

String topic = "/default/test_topic";
String consumerName = "test_consumer";
String subscriptionName = "test_subscription";

Consumer consumer = client.newConsumer()
        .withTopic(topic)
        .withConsumerName(consumerName)
        .withSubscription(subscriptionName)
        .withSubscriptionType(SubType.EXCLUSIVE)
        .build();

// Subscribe to the topic
consumer.subscribe();
System.out.printf("The Consumer %s was created%n", consumerName);

CountDownLatch shutdown = new CountDownLatch(1);

// Start receiving messages via Flow.Publisher
consumer.receive().subscribe(new Flow.Subscriber<>() {
    @Override
    public void onSubscribe(Flow.Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(StreamMessage msg) {
        System.out.printf("Received message: %s%n", new String(msg.payload()));

        // Acknowledge the message
        consumer.ack(msg);
    }

    @Override public void onError(Throwable t) { shutdown.countDown(); }
    @Override public void onComplete() { shutdown.countDown(); }
});

shutdown.await();
client.close();
```

### Schema Registry

```java
import com.danubemessaging.client.DanubeClient;
import com.danubemessaging.client.Producer;
import com.danubemessaging.client.SchemaRegistryClient;
import com.danubemessaging.client.schema.SchemaType;
import java.util.Map;

DanubeClient client = DanubeClient.builder()
        .serviceUrl("http://127.0.0.1:6650")
        .build();

SchemaRegistryClient schemaClient = client.newSchemaRegistry();

String jsonSchema = """
        {
          "type": "object",
          "properties": {
            "field1": {"type": "string"},
            "field2": {"type": "integer"}
          }
        }""";

// Register a JSON schema
schemaClient.registerSchema(
        schemaClient.newRegistration()
                .withSubject("my-app-events")
                .withSchemaType(SchemaType.JSON_SCHEMA)
                .withSchemaDefinition(jsonSchema.getBytes()));

// Create producer with schema reference
Producer producer = client.newProducer()
        .withTopic("/default/test_topic")
        .withName("schema_producer")
        .withSchemaLatest("my-app-events")
        .build();

producer.create();
```

Browse the [examples directory](https://github.com/danube-messaging/danube-java/tree/main/examples) for complete working code.

## Contribution

Working on improving and adding new features. Please feel free to contribute or report any issues you encounter.

### Running Integration Tests

Before submitting a PR, start the test cluster and run the integration tests:

```bash
# 1. Start the cluster
cd docker/
docker compose up -d

# 2. Wait for the broker to be healthy
docker compose ps

# 3. Run the integration tests from the repository root
cd ..
mvn -pl danube-client -Pintegration-tests verify

# 4. Stop the cluster when done
cd docker/
docker compose down -v
```

### Repository layout

- `danube-client-proto` — generated protobuf + gRPC Java stubs
- `danube-client` — handwritten Java client API and internals

### Regenerating gRPC stubs

Make sure the proto files are the latest from the [Danube project](https://github.com/danube-messaging/danube/tree/main/danube-core/proto).

Copy all `.proto` files into:

```
danube-client-proto/src/main/proto/
  DanubeApi.proto
  SchemaRegistry.proto
```

No manual `protoc` install is required — Maven downloads the compiler and plugin automatically. Regenerate from the repo root:

```bash
mvn -pl danube-client-proto -am generate-sources
```

Or via the helper script:

```bash
bash scripts/generate-stubs.sh
```

Generated sources will be in:

- `danube-client-proto/target/generated-sources/protobuf/java`
- `danube-client-proto/target/generated-sources/protobuf/grpc-java`

### Build all modules

```bash
mvn clean verify
```
