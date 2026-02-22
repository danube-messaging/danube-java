# Danube Java Examples

Standalone Java examples demonstrating common Danube messaging client workflows.
Each file is a self-contained program with a `main` method — no build system required.

## Examples

| File | Description |
|------|-------------|
| `SimpleProducerConsumer.java` | Basic producer + consumer, raw byte messages |
| `JsonProducer.java` | Register a JSON schema and produce schema-tagged messages |
| `JsonConsumer.java` | Consume and print JSON messages |
| `PartitionsProducer.java` | Produce across 3 topic partitions |
| `PartitionsConsumer.java` | Consume from all partitions, showing which partition each message arrived from |
| `ReliableDispatchProducer.java` | Reliable dispatch: broker waits for ack before sending next message |
| `ReliableDispatchConsumer.java` | Consume reliable messages and report byte throughput |
| `SchemaEvolution.java` | Register, evolve, and compatibility-check schemas in the registry |

## Prerequisites

Start the Danube broker using Docker Compose:

```bash
cd docker
docker compose up -d
```

Build the client jars and fetch all runtime dependencies:

```bash
# From the repo root
mvn -DskipTests package
mvn -pl danube-client dependency:copy-dependencies -DoutputDirectory=target/dependency -DincludeScope=runtime -q
```

## Running an Example

Build the full classpath (all transitive deps + client jars):

```bash
CP=$(find danube-client/target/dependency -name "*.jar" | tr '\n' ':')$(find danube-client/target danube-client-proto/target -maxdepth 1 -name "*.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" | tr '\n' ':')

# Compile
mkdir -p examples/out
javac -cp "$CP" examples/SimpleProducerConsumer.java -d examples/out

# Run
java -cp "$CP:examples/out" SimpleProducerConsumer
```

Override the broker URL via environment variable if needed:

```bash
DANUBE_BROKER_URL=http://my-broker:6650 java -cp "$CP:examples/out" SimpleProducerConsumer
```

## Producer / Consumer Pairs

These examples are designed to be run together in separate terminals:

| Producer | Consumer | Topic |
|----------|----------|-------|
| `JsonProducer` | `JsonConsumer` | `/default/json_topic` |
| `PartitionsProducer` | `PartitionsConsumer` | `/default/partitioned_topic` |
| `ReliableDispatchProducer` | `ReliableDispatchConsumer` | `/default/reliable_topic` |

**Always start the consumer first** so it is subscribed before the producer sends.

## Key Concepts

- **`producer.create()`** — registers the producer with the broker (creates topic if needed)
- **`consumer.subscribe()`** — subscribes to the topic, starts receive loops on virtual threads
- **`consumer.receive()`** — returns a `Flow.Publisher<StreamMessage>`; attach a `Flow.Subscriber` to receive messages
- **`consumer.ack(msg)`** — acknowledges a message; required in reliable dispatch mode
- **Schema registry** — register schemas via `client.newSchemaRegistry()`, then use `.withSchemaLatest(subject)` on the producer builder
