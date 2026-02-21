# Danube Java examples

These standalone examples demonstrate common Danube Java client workflows.

## Files

- `SchemaRegistryProducerExample.java`: register schema + build producer with schema reference
- `ConsumerObservabilityExample.java`: subscribe consumer with lifecycle/message/error listeners

## Run locally

1. Start a Danube broker locally.
2. Build artifacts:

```bash
mvn -DskipTests package
```

3. Compile and run an example with your preferred IDE or local `javac/java` setup, ensuring `danube-client` and `danube-client-proto` are on the classpath.
