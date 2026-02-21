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
