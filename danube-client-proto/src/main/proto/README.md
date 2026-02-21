# Proto files location

Copy Danube `.proto` files into this directory while preserving relative import paths.

Example:

```text
src/main/proto/
  DanubeApi.proto
  SchemaRegistry.proto
  ...
```

Code generation is wired in Maven via `protobuf-maven-plugin`.
