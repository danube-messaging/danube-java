#!/usr/bin/env bash
set -euo pipefail

# Generates Java protobuf + gRPC stubs into danube-client-proto/target/generated-sources
mvn -pl danube-client-proto -am clean generate-sources

echo "Generated sources:" 
echo "  - danube-client-proto/target/generated-sources/protobuf/java"
echo "  - danube-client-proto/target/generated-sources/protobuf/grpc-java"
