# Danube Java Client — Implementation Plan

## 1) Objectives

Build a modern, high-performance Java client for Danube that feels consistent with the Go and Rust clients while using modern Java 21 capabilities.

**Primary goals**
- API ergonomics similar to Go/Rust builders.
- High throughput and resilience for producer/consumer flows.
- Minimal dependency surface in core client.
- GraalVM Native Image friendly design.
- Clear progress tracking from day 1.

---

## 2) Decisions Locked In

- **Java target:** Java 21 (LTS)
- **Build tool:** Maven
- **Architecture:** Vanilla Java (no Spring in core)
- **Async model:** Virtual Threads for I/O + background loops
- **Public async API:** `CompletableFuture<T>`
- **Internal task orchestration:** `StructuredTaskScope`
- **Context propagation:** `ScopedValue` (instead of `ThreadLocal`)
- **Public receive API:** `Flow.Publisher<StreamMessage>`
- **Modern language usage:** records, switch expressions, pattern matching
- **Native-friendliness:** avoid reflection-heavy frameworks
- **Scope:** include all major capabilities in Go/Rust clients

### Naming (domain-compatible)
- **Maven groupId:** `com.danubemessaging`
- **Artifact ID (client):** `danube-client`
- **Java base package:** `com.danubemessaging.client`

> Note: Java package names cannot contain hyphens, so `com.danube-messaging` is invalid.

### Transport & logging choices
- **gRPC transport:** `grpc-netty-shaded` (most common/default choice for high-throughput Java clients)
- **Logging API:** `slf4j-api` (widely adopted and user-friendly integration with any backend)

---

## 3) Maven Module Strategy

## Modules
1. `danube-client-proto`
   - Owns protobuf + gRPC Java code generation.
   - No handwritten business logic.

2. `danube-client`
   - Handwritten API and runtime internals.
   - Depends on `danube-client-proto`.

## Proto location
Place proto files under:

`danube-client-proto/src/main/proto/`

(keep folder hierarchy/import paths exactly as in source repos)

## Do you need to publish 2 artifacts?
**Internally:** yes, there are 2 Maven modules.

**For users:** usually they add only **one dependency**:

- `com.danubemessaging:danube-client`

because `danube-client` pulls `danube-client-proto` transitively.

So operationally for users it stays simple (single dependency).

---

## 4) Public API Blueprint (Go/Rust feel)

```java
var client = DanubeClient.builder()
    .serviceUrl("http://127.0.0.1:6650")
    .withApiKey("...")
    .build();

var producer = client.newProducer()
    .withTopic("/default/topic")
    .withName("producer-1")
    .build();

producer.createAsync().join();
producer.sendAsync("hello".getBytes(), Map.of()).join();

var consumer = client.newConsumer()
    .withTopic("/default/topic")
    .withConsumerName("consumer-1")
    .withSubscription("sub-1")
    .withSubscriptionType(SubType.EXCLUSIVE)
    .build();

consumer.subscribeAsync().join();
Flow.Publisher<StreamMessage> publisher = consumer.receive();
```

### Planned top-level types
- `DanubeClient`, `DanubeClientBuilder`
- `Producer`, `ProducerBuilder`, `ProducerOptions`
- `Consumer`, `ConsumerBuilder`, `ConsumerOptions`, `SubType`
- `SchemaRegistryClient`, `SchemaRegistrationBuilder`
- Re-export/bridge of schema reference types as needed

---

## 5) Internal Architecture Plan

## Core services
- `ConnectionManager`
  - Channel cache keyed by `(brokerUrl, connectUrl, proxy)`
  - TLS/mTLS/API-key aware channel creation

- `AuthService`
  - Authenticate API key -> bearer token
  - Cache token + expiry, refresh on demand

- `LookupService`
  - Topic lookup (redirect/connect/failed)
  - Topic partitions lookup

- `HealthCheckService`
  - Background health check loop
  - Signals producer/consumer stop/re-subscribe conditions

- `RetryManager`
  - Retryable status classification:
    - `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`
  - Jittered backoff with caps
  - Proxy metadata helper (`x-danube-broker-url`)

## Producer path
- `Producer` manages one or many `TopicProducer` instances.
- Non-partitioned topic => one topic producer.
- Partitioned topic => N topic producers + round-robin router.
- Send flow:
  - retry transient errors
  - recreate on unrecoverable
  - lookup + recreate when retries exhausted
- Schema metadata resolved once at create when schema reference is set.

## Consumer path
- `Consumer` resolves topic partitions, creates per-partition `TopicConsumer`s.
- Uses `StructuredTaskScope` for startup fan-out and receive orchestration.
- Unified receive stream exposed as `Flow.Publisher<StreamMessage>`.
- Ack routes to correct partition consumer by message topic.
- Resubscribe strategy on stop signal / unrecoverable / retry exhaustion.

---

## 6) Package Layout (proposed)

```text
com.danubemessaging.client
  ├─ api/
  │   ├─ DanubeClient.java
  │   ├─ Producer.java
  │   ├─ Consumer.java
  │   ├─ SchemaRegistryClient.java
  │   └─ ...
  ├─ builder/
  │   ├─ DanubeClientBuilder.java
  │   ├─ ProducerBuilder.java
  │   └─ ConsumerBuilder.java
  ├─ internal/
  │   ├─ auth/
  │   ├─ connection/
  │   ├─ lookup/
  │   ├─ health/
  │   ├─ retry/
  │   ├─ producer/
  │   ├─ consumer/
  │   └─ routing/
  ├─ schema/
  │   ├─ SchemaRegistrationBuilder.java
  │   ├─ SchemaInfo.java
  │   └─ ...
  ├─ model/
  │   ├─ StreamMessage.java
  │   ├─ MessageId.java
  │   └─ ...
  └─ errors/
```

---

## 7) Phased Delivery Plan

## Phase 0 — Project scaffolding
- Parent POM + 2 modules.
- Java 21 compiler/toolchain setup.
- Protobuf/gRPC generation in `danube-client-proto`.
- Basic CI workflow (build + test).

**Exit criteria**
- `mvn -q -DskipTests package` succeeds.
- Generated stubs available to `danube-client`.

## Phase 1 — Client foundation
- `DanubeClientBuilder` with service URL, TLS, mTLS, API key.
- `ConnectionManager`, `AuthService`, `LookupService`.
- `RetryManager` primitives.

**Exit criteria**
- Can authenticate and lookup topic/partitions successfully.

## Phase 2 — Producer MVP
- `ProducerBuilder`, `Producer`, `TopicProducer`.
- `createAsync`, `sendAsync` with retries + recreate logic.
- Partitioned/non-partitioned support.

**Exit criteria**
- Publish messages in simple + partitioned examples.

## Phase 3 — Consumer MVP
- `ConsumerBuilder`, `Consumer`, `TopicConsumer`.
- `subscribeAsync`, `receive()` as `Flow.Publisher`, `ackAsync`.
- Receive loops + resubscribe behavior.

**Exit criteria**
- Receive and ack messages in simple + partitioned examples.

## Phase 4 — Health check + hardening
- Health check lifecycle integration for producer/consumer.
- Stop signal handling and reconnect edge cases.
- More robust error classification and state transitions.

**Exit criteria**
- Stable operation through broker restart/rebalance scenarios.

## Phase 5 — Schema registry + schema references
- `SchemaRegistryClient` methods:
  - register/get latest/get by id/get version/list versions
  - compatibility and topic schema config operations
- Producer schema ref resolution:
  - latest, pinned version, min version

**Exit criteria**
- Schema examples equivalent to Go/Rust pass.

## Phase 6 — UX polish, docs, and release prep
- README with quick start and advanced usage.
- Javadoc with Markdown formatting.
- Example apps parity with Go/Rust.
- Publishing setup for Maven Central.

**Exit criteria**
- First beta release candidate ready.

---

## 8) Validation Matrix

- **Functional parity:** create/send/subscribe/receive/ack/schema flows
- **Resilience:** retry/reconnect/relookup/stop-signal behavior
- **Performance:** throughput/latency under virtual threads
- **Compatibility:** Java 21 runtime and GraalVM native checks
- **Ergonomics:** simple happy-path API with fluent builders

---

## 9) Progress Tracker

Use this section as the live checklist during implementation.

- [x] Phase 0: Multi-module scaffold complete
- [x] Phase 1: Core client foundation complete
- [x] Phase 2: Producer implementation complete (integration tests deferred)
- [x] Phase 3: Consumer implementation complete (integration tests deferred)
- [x] Phase 4: Health check + hardening complete
- [x] Phase 5: Schema registry + schema refs complete
- [x] Phase 6: Docs/examples/release prep complete

### Current status
- **Overall:** Phases 4, 5, and 6 implementation complete.
- **Next immediate step:** Execute deferred integration test matrix for resilience + schema parity.

### Validation note
- Integration tests (producer/consumer parity with Go/Rust) are intentionally deferred and will be implemented together in the dedicated integration-test pass.
