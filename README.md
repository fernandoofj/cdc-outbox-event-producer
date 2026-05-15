# cdc-outbox-event-producer

> **Status:** repository revived from the public archive
> [`inventa-shop/kotlin-postgres-cdc-to-sns-module`](https://github.com/inventa-shop/kotlin-postgres-cdc-to-sns-module)
> (last commit Feb/2024) and re-published privately to evolve the design.
> The code as-is still works; everything below from
> [Roadmap](#roadmap) onward is a forward-looking plan.

A Kotlin/JVM library that turns a PostgreSQL database into a **transactional
outbox event producer**: it tails the WAL, picks up messages emitted with
`pg_logical_emit_message(...)` inside the same transaction as the business
write, and publishes them to AWS SNS / SQS.

Despite the original "CDC" naming, the module does **not** propagate raw
`INSERT/UPDATE/DELETE` row events. It implements the **Transactional Outbox
Pattern** with **zero outbox table**: the outbox row is replaced by a logical
WAL message that lives only in the replication stream. Insert/update/delete
events that arrive in the slot are deliberately discarded
([SlotReaderMessageProducer.kt:124](src/main/kotlin/br/com/fltech/cdc/outbox/publisher/workflow/SlotReaderMessageProducer.kt:124)).

---

## Table of contents

1. [How it works today](#how-it-works-today)
2. [Quick start](#quick-start)
3. [Prefix routing convention](#prefix-routing-convention)
4. [Configuration reference](#configuration-reference)
5. [Honest assessment of the current code](#honest-assessment-of-the-current-code)
6. [Alternatives in the ecosystem](#alternatives-in-the-ecosystem)
7. [Roadmap](#roadmap)
8. [Target architecture (hexagonal)](#target-architecture-hexagonal)
9. [Testing strategy](#testing-strategy)
10. [Build & local dev](#build--local-dev)

---

## How it works today

```
┌──────────────────────────┐    pg_logical_emit_message      ┌─────────────────┐
│ Application transaction  │ ─────────────────────────────▶  │ Postgres WAL    │
│ (INSERT + emit_message)  │                                 │ (logical slot)  │
└──────────────────────────┘                                 └────────┬────────┘
                                                                      │ wal2json v2
                                                                      ▼
                              ┌────────────────────────────────────────────────┐
                              │  PostgresConnector (JDBC replication API)      │
                              │   ├── streamingConnection (replication = true) │
                              │   └── queryConnection     (regular SQL)        │
                              └────────────────────────────────────────────────┘
                                                  │
                                                  ▼  ByteBuffer → MessageChange
                              ┌────────────────────────────────────────────────┐
                              │  SlotReaderMessageProducer (blocking loop)     │
                              │   parsePrefix("SNS|topic" | "SQS|queue")       │
                              └────────────────────────────────────────────────┘
                                                  │
                              ┌────────────────────────────┐
                              ▼                            ▼
                   SNSTransactionalProducer       SQSTransactionalProducer
                  (Spring Cloud AWS Messaging 2.4 — deprecated, see roadmap)
```

Flow:

1. The application opens a JDBC transaction and, inside it, calls
   `SELECT pg_logical_emit_message(true, '<prefix>', '<json content>')`
   together with the business `INSERT`/`UPDATE`. Both reach the WAL
   atomically — that is the outbox guarantee.
2. `PostgresConnector` keeps **two** raw `DriverManager` connections:
   one in replication mode (`replication=database`), one for regular queries
   such as `pg_current_wal_lsn()`. It creates the logical slot with the
   `wal2json` output plugin and `format-version=2` by default.
3. `SlotReaderMessageProducer.startStreaming()` is an unbounded
   `while (running)` loop that calls `readPending()`, parses the JSON
   payload via `ByteToClassParserImplV2`, and routes by prefix.
4. After a successful publish, the LSN is advanced
   (`setAppliedLSN` + `setFlushedLSN`) so Postgres can recycle WAL.
5. Failures are logged but do **not** retry the publish (see
   [Honest assessment](#honest-assessment-of-the-current-code) §3).

---

## Quick start

### Producer side — emit the outbox message

Raw SQL (works from any client):

```sql
SELECT pg_logical_emit_message(
    true,                   -- transactional
    'SNS|orders-events',    -- prefix (= destination route)
    '{"eventType":"OrderPlaced","domainId":"01HX...","payload":{...}}'
);
```

From Spring Data JPA (recommended on the producer side):

```kotlin
@Repository
interface OutboxRepository : JpaRepository<Order, Long> {

    @Modifying
    @Query(
        nativeQuery = true,
        value = "SELECT CAST(pg_logical_emit_message(:tx, :prefix, :content) AS VARCHAR)"
    )
    fun emitLogicalMessage(
        @Param("tx") transactional: Boolean,
        @Param("prefix") prefix: String,
        @Param("content") content: String
    ): String
}
```

### Consumer side — run the streamer

```kotlin
SlotReaderMessageProducer(
    PostgresConfiguration(
        host = "localhost", port = "5432", database = "appdb",
        username = "replica", password = "***",
    ),
    ReplicationConfiguration(slotName = "outbox_slot"),
    snsProducer = SNSTransactionalProducer(notificationMessagingTemplate),
    sqsProducer = SQSTransactionalProducer(queueMessagingTemplate),
).startStreaming()
```

> Run this on **exactly one** instance per slot. The slot is single-consumer
> by design; multiple readers will fight for it. See
> [`PostgresConnector.handleCurrentlyRunningProcessOnSlotException`](src/main/kotlin/br/com/fltech/cdc/outbox/publisher/replication/connector/PostgresConnector.kt:136)
> for the back-off behavior.

### Postgres prerequisites

```
wal_level = logical
max_replication_slots >= 1 per producer
max_wal_senders     >= 1 per producer
```

…and a role with `REPLICATION` and `LOGIN`. The `wal2json` plugin must be
installed (the `debezium/postgres` image used in `docker-compose.yml` ships
with it).

---

## Prefix routing convention

The destination is encoded in the WAL message prefix:

| Prefix                  | Routes to        | Notes                                |
|-------------------------|------------------|--------------------------------------|
| `topic-name`            | SNS topic        | Bare prefix defaults to SNS          |
| `SNS\|topic-name`       | SNS topic        | Explicit form                        |
| `SQS\|queue-name`       | SQS queue        | Pipe `\|` separates type and name    |

Parsed at [`SlotReaderMessageProducer.parsePrefix`](src/main/kotlin/br/com/fltech/cdc/outbox/publisher/workflow/SlotReaderMessageProducer.kt:154).
The hard-coded enum (`DestinationType.SNS|SQS`) and pipe separator are the
first thing the [hexagonal refactor](#target-architecture-hexagonal) replaces.

---

## Configuration reference

### `PostgresConfiguration`

| Field             | Default     | Purpose                                  |
|-------------------|-------------|------------------------------------------|
| `host` / `port`   | — / `5432`  |                                          |
| `database`        | —           |                                          |
| `username`        | —           | Must have `REPLICATION` privilege        |
| `password`        | —           |                                          |
| `sslMode`         | `disable`   | `disable\|require\|verify-ca\|verify-full` |
| `pathToRootCert`  | `null`      | Server CA bundle for `verify-*`          |
| `pathToSslCert`   | `null`      | Client certificate (mTLS)                |
| `pathToSslKey`    | `null`      | Client key                               |
| `sslPassword`     | `null`      | Key passphrase                           |

### `ReplicationConfiguration`

| Field                              | Default                  | Purpose                                          |
|------------------------------------|--------------------------|--------------------------------------------------|
| `slotName`                         | —                        | Replication slot name (unique per producer)      |
| `outputPlugin`                     | `wal2json`               | Postgres output plugin                           |
| `statusIntervalValue`/`Unit`       | `20`, `SECONDS`          | Keep-alive cadence to Postgres                   |
| `updateIdleSlotInterval`           | `300 s`                  | Force-flush LSN on idle to free WAL              |
| `existingProcessRetryLimit`        | `30`                     | Max retries when another reader holds the slot   |
| `existingProcessRetrySleepSeconds` | `30 s`                   | Fixed sleep between retries (overrides backoff)  |
| `includeXids`                      | `true`                   | wal2json: include transaction ids                |
| `formatVersion`                    | `V2`                     | wal2json format-version: `V1` or `V2`            |

---

## Honest assessment of the current code

Findings from a top-down read of [`src/main/kotlin/...`](src/main/kotlin/br/com/fltech/cdc/outbox/publisher):

1. **Connection management is bare.**
   `DefaultConnectionProvider` calls `DriverManager.getConnection` directly
   ([DefaultConnectionProvider.kt:7](src/main/kotlin/br/com/fltech/cdc/outbox/publisher/replication/connector/DefaultConnectionProvider.kt:7)).
   No pooling for the query connection, no TCP keep-alive, no socket
   timeout, no `connectTimeout`. A network blip during connect blocks
   indefinitely.

2. **Reconnect is "throw it all away and start over".**
   Any `SQLException` in the streaming loop unwinds the entire
   `use { }` block and reconstructs the connector
   ([SlotReaderMessageProducer.kt:52](src/main/kotlin/br/com/fltech/cdc/outbox/publisher/workflow/SlotReaderMessageProducer.kt:52)).
   Only the `57P03` (recovery mode) state has a tailored sleep — every
   other failure spins immediately. There is **no exponential back-off** on
   the reconnect path, only on the "slot already in use" path.

3. **Delivery guarantee has a latent bug.**
   The intent is at-least-once: the LSN advances only after `setStreamLsn`
   inside `onSNSSuccess`/`onSQSSuccess`. But if message **N** fails and
   message **N+1** succeeds, `onSuccess` will call
   `setAppliedLSN(lastReceivedLsn)` — which is the LSN of N+1. **Message N
   is silently dropped** ([SlotReaderCallback.kt:43](src/main/kotlin/br/com/fltech/cdc/outbox/publisher/workflow/SlotReaderCallback.kt:43)).
   Correct behavior is to halt advancement on failure, retry with
   back-off, and only resume the LSN advance after the failing message
   succeeds or is dead-lettered.

4. **Single-threaded throughput ceiling.**
   One loop, one publish per iteration, no batching, no parallel sinks.
   Throughput is bounded by `max(broker_publish_latency)`.

5. **`running` is not `@Volatile`.**
   `stopStreaming()` may take a while to be observed.
   ([SlotReaderMessageProducer.kt:30](src/main/kotlin/br/com/fltech/cdc/outbox/publisher/workflow/SlotReaderMessageProducer.kt:30))

6. **Spring Cloud AWS Messaging 2.4.4 is end-of-life.**
   `io.awspring.cloud:spring-cloud-aws-messaging:2.4.4` was deprecated
   in favor of Spring Cloud AWS **3.x** (`spring-cloud-aws-starter-sns`,
   `spring-cloud-aws-starter-sqs`). The 2.x line uses AWS SDK v1, also EOL.

7. **No metrics, no health indicator, no tracing.**
   Operability is `grep`-on-logs only. We need at minimum:
   - `cdc.outbox.read{slot}` counter
   - `cdc.outbox.published{sink,topic}` counter
   - `cdc.outbox.publish.duration{sink}` timer
   - `cdc.outbox.lag.bytes` gauge (`pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`)
   - `HealthIndicator` that turns DOWN when the slot is inactive longer
     than a threshold or when the publisher is failing.

8. **Prefix routing is a stringly-typed pipe-split.**
   Adding Kafka/RabbitMQ means changing the `DestinationType` enum *and*
   the `processMessage` switch *and* the constructor of
   `SlotReaderMessageProducer` — three coupled edits per new sink.

9. **Hard Spring annotations on the leaf classes.**
   `@Component` on `SNSTransactionalProducer`, `SQSTransactionalProducer`,
   `ByteToClassParserStrategy`, `SlotReaderCallback` — but
   `SlotReaderMessageProducer` itself is **not** annotated. The wiring is
   inconsistent: half-Spring, half-DIY. Either go all-in (auto-config) or
   keep the core framework-free and ship a separate Spring starter.

10. **Toolchain is dated.**
    - Kotlin 1.7.20 → current is 2.x
    - JVM 17 is fine, but Spring Boot 3.5 already needs Java 21 in some
      paths
    - `aws-java-sdk-sts:1.12.x` (v1 SDK) + `software.amazon.awssdk:sts:2.21.1`
      (v2 SDK) **both** included — pick one (v2)
    - `gradle-versions-plugin` is configured but no policy enforces it.

11. **Outbox semantics conflate two patterns.**
    The README example (`SaveOutboxMessagePort.emitLogicalMessage`) is
    table-less; the name "outbox" usually means a real table with a
    poller. Documentation needs to clearly state: *this is the WAL-message
    variant, not the polled-table variant*.

---

## Alternatives in the ecosystem

A quick survey before reinventing anything (item 1 of the brief):

| Project | What it is | Why it isn't a drop-in replacement |
|---|---|---|
| **Debezium Engine** (`io.debezium:debezium-embedded`) | Java library that runs a Debezium connector in-process and delivers `ChangeEvent`s to a `Consumer`. Pluggable sinks. | Closest off-the-shelf. Heavy (~30 MB transitive), opinionated about offset storage, and oriented at **row-level** CDC — to do the "logical message" trick you still need extra plumbing. Worth wrapping as one of our `CdcSource` adapters. |
| **Debezium Server** | Standalone runtime that emits to Kinesis, Pulsar, RabbitMQ, Pub/Sub, NATS, Redis, etc. | Sidecar process, not a library. Heavier ops footprint, separate config surface. |
| **Eventuate Tram** | Spring-based outbox + CDC framework (JDBC poller + Kafka/RabbitMQ/Redis publishers). | Mature but framework-y, ties you to Eventuate's message envelope and saga model. |
| **Spring Modulith Events** (since 1.0, 2023) | Built-in transactional event publication with an outbox table + republisher; pluggable broker via `Externalized`. | Only useful inside a Spring Modulith app; not a generic CDC library. |
| **`mysql-binlog-connector-java`** | Low-level binlog reader for MySQL. | Library, not framework — needs the same wrapping we are designing here. Will become our MySQL adapter. |
| **`pgjdbc` logical replication API** | What the current code uses. | Lowest-level building block. We keep this as the Postgres adapter. |
| **Striim / Maxwell / DBLog** | Commercial / standalone runtimes. | Out of scope for an embeddable library. |

**Conclusion.** There is **no** existing Kotlin/Spring library that does
exactly what this module does (WAL-message outbox → SNS/SQS/Kafka/RabbitMQ)
with a clean hexagonal split. Debezium Engine is the closest, but solves
a slightly different problem (row-level CDC). It makes sense to keep this
project and add a `DebeziumEngineCdcSource` adapter for the row-level case.

---

## Roadmap

Mapped to the items in the brief plus follow-ups raised in review:

| # | Theme | Deliverable | Wave |
|---|---|---|---|
| 1 | Survey existing libs | [§ Alternatives](#alternatives-in-the-ecosystem) | done in this README |
| 2 | Code quality: pool, reconnect, delivery, observability | HikariCP for the query connection (wired as default); back-off + jitter on every reconnect with a configurable attempt cap; `@Volatile` running flag + cooperative interrupt-aware shutdown; Micrometer counters/timers (no-op when no registry); LSN skip-on-failure bug fixed at the callback API level; idle-flush no longer fast-forwards past pending failures. | Wave 1 — done |
| 2a | Quality follow-ups not closed in Wave 1 | True per-message LSN extraction from `nextlsn` (wal2json) / pgoutput protocol header — closes the residual race between `readPending()` and `lastReceiveLSN()`; head-of-line retry with DLQ (depends on the `EventSink` port from Wave 4); Spring Boot `HealthIndicator` (belongs in Wave 2's starter); Testcontainers integration regression for at-least-once. | Wave 1.5 |
| 3 | Spring Boot integration | `cdc-outbox-spring-boot-starter` (Boot 3.5 / AWS Spring Cloud 3.x), `@ConfigurationProperties`, `SmartLifecycle`, conditional auto-config per sink | Wave 2 |
| 4 | Multi-DB via hexagonal | `core` module exposing `CdcSource` port; `adapter-postgres` (modernized, pgoutput option, PG16+), `adapter-mysql` (binlog + outbox table fallback). Stubs for `adapter-sqlserver` (CT/CDC) and `adapter-oracle` (LogMiner / OpenLogReplicator) | Wave 3 |
| 5 | Multi-broker via hexagonal | `EventSink` port; adapters: `adapter-sink-sns`, `adapter-sink-sqs`, `adapter-sink-kafka`, `adapter-sink-rabbitmq`. Composite + Router sinks for fan-out and migration scenarios | Wave 4 |
| 6 | Tests | Unit (codec, routing, retry, sinks with mocked clients) + integration via Testcontainers matrix (PG/MySQL × SNS/SQS/Kafka/RabbitMQ) + fault-injection (broker outage, DB restart, slot conflict) | continuous |
| 7 | **Configurable table / field mapping** | Declarative `TableMapping` that selects which tables and columns flow through the producer, how raw column changes (`I/U/D`) are translated into outbound `OutboxEvent`s (eventType derivation, payload projection, key extraction, header attributes), and which sink/topic each table targets. Same surface used by both the WAL-message and row-level CDC flavours. Plays well with item 4 (multi-DB) and item 5 (multi-broker). | Wave 3.5 |

Wave boundaries are deliberate so each wave merges to `main` independently
and the library stays usable in between.

### Item 7 — flexibility of table / field mapping

Concretely, the library will expose a typed configuration block of the form:

```yaml
cdc:
  outbox:
    mappings:
      - table: public.orders                  # FQ table name (schema.table)
        capture: [I, U, D]                    # which ops to forward (default: I,U,D)
        key:                                  # how to derive the partition / domain key
          columns: [id]
          format: "{id}"                      # template; defaults to first column
        payload:                              # which columns become the event payload
          include: [id, status, total_cents, updated_at]
          exclude: []                         # mutually exclusive with include
          rename:                             # column → JSON field
            total_cents: totalCents
            updated_at: updatedAt
        eventType:                            # how to build the event type string
          template: "orders.{op}"             # {op}=created|updated|deleted
        routing:
          sink: sns://orders-events           # explicit; otherwise falls back to a default
          attributes:                         # passed through as SNS MessageAttributes / Kafka headers
            tenant: "{tenant_id}"
      - table: public.invoices
        ...                                   # one block per captured table
```

The same `TableMapping` model is used by:

- The Postgres logical-replication source — when the configured output
  plugin emits row-level changes (`I/U/D`), the mapping decides what
  becomes an `OutboxEvent`.
- The MySQL binlog source.
- The outbox-table poller variant — the mapping describes the
  `outbox_events` table schema.

Behaviour parity is enforced by the same test suite running against
each source adapter.

---

## Target architecture (hexagonal)

```
cdc-outbox-event-producer/
├── core/                                    pure Kotlin, no Spring, no I/O
│   ├── domain/
│   │   ├── OutboxEvent.kt                   (id, prefix, headers, payload, occurredAt, source LSN/GTID)
│   │   ├── Routing.kt                       (target, partitionKey, attributes)
│   │   └── Ack.kt                           (opaque token to commit progress)
│   ├── port/
│   │   ├── in/
│   │   │   └── CdcSource.kt                 open(), poll(timeout): Batch, ack(batch), close()
│   │   ├── out/
│   │   │   ├── EventSink.kt                 publish(routing, event): PublishResult
│   │   │   └── EventSinkRegistry.kt         resolve(scheme): EventSink
│   │   └── support/
│   │       ├── EventCodec.kt                bytes ↔ OutboxEvent
│   │       └── BackOff.kt                   pluggable retry policy
│   └── application/
│       └── CdcProcessor.kt                  orchestrates source → codec → sink → ack
│
├── adapter-source-postgres/                 wraps current PostgresConnector, modernized
│   ├── PgLogicalReplicationCdcSource.kt     wal2json + pgoutput
│   └── PgConnectionFactory.kt               HikariCP for query connection
│
├── adapter-source-mysql/
│   ├── MySqlBinlogCdcSource.kt              mysql-binlog-connector-java
│   └── MySqlOutboxTableCdcSource.kt         SKIP LOCKED poller (MySQL 8+)
│
├── adapter-source-sqlserver/                stub: Change Tracking / CT
├── adapter-source-oracle/                   stub: LogMiner / XStream
│
├── adapter-sink-sns/                        AWS SDK v2
├── adapter-sink-sqs/                        AWS SDK v2
├── adapter-sink-kafka/                      kafka-clients
├── adapter-sink-rabbitmq/                   amqp-client
├── adapter-sink-composite/                  fan-out
├── adapter-sink-router/                     prefix scheme → sink resolution (sns://, kafka://…)
│
├── observability/                           Micrometer meters, HealthIndicator
│
└── spring-boot-starter/                     auto-configuration, properties, conditionals
```

**Key invariants of the port design:**

- The **`OutboxEvent`** is the *only* type that crosses the hexagon
  boundary. Adapters translate to/from native shapes.
- **`CdcSource`** delivers events in a `Batch` with an opaque `Ack`
  token. The processor only acks after **every** event in the batch is
  published — this fixes the latent skip-on-failure bug from item 3 of
  the assessment.
- **`EventSinkRegistry`** replaces the pipe-split prefix parser. Routing
  becomes a URL-like scheme: `sns://topic`, `sqs://queue`,
  `kafka://cluster-a/topic`, `amqp://exchange/routingKey`. Adding a new
  broker = drop a jar on the classpath + register one bean.
- **`CompositeSink`** lets a single event fan out to N sinks (dual-write
  during a migration); **`RouterSink`** picks one sink based on the
  scheme. Both are pure composition — no inheritance.

### Spring Boot starter outline

```yaml
cdc:
  outbox:
    enabled: true
    source:
      type: postgres            # postgres | mysql-binlog | mysql-outbox | debezium
      postgres:
        host: localhost
        slot-name: outbox_slot
        output-plugin: wal2json
        format-version: V2
        connection-pool:
          maximum-pool-size: 4
          connect-timeout: 5s
    sinks:
      sns:
        region: sa-east-1
      kafka:
        bootstrap-servers: kafka:9092
        acks: all
        enable-idempotence: true
      rabbitmq:
        host: rabbit
        confirm-callback: true
    retry:
      max-attempts: 8
      initial-backoff: 200ms
      max-backoff: 30s
      jitter: 0.3
    dead-letter:
      sink: sqs://outbox-dlq
```

The starter wires a `CdcProcessor` bean per configured source, plus the
`EventSinkRegistry` from whichever `adapter-sink-*` jars are on the
classpath.

### Multi-DB notes

- **PostgreSQL.** Move the floor to **PG 14+** (current default) but
  document tested versions through PG 17. Offer `pgoutput` as an
  alternative to `wal2json` — it ships in core and removes the plugin
  install requirement. Keep `wal2json` for the rich JSON format.
- **MySQL.** Two adapters:
  1. **Binlog reader.** Captures row events from `mysqlbinlog` style
     streams via `mysql-binlog-connector-java`. Requires `ROW` binlog
     format, `binlog_row_metadata=FULL`, `GTID_MODE=ON`. Outbox is then
     an INSERT into an `outbox_events` table whose row events we filter.
  2. **Outbox table poller.** Classic `SELECT ... FOR UPDATE SKIP LOCKED`
     (MySQL 8+). Lower setup cost, slightly higher latency.
  Default: poller. Binlog reader for high-throughput workloads.
- **SQL Server / Oracle.** Designed-in but not implemented in Wave 3:
  - SQL Server: Change Tracking (cheap) or CDC (richer).
  - Oracle: LogMiner (deprecated path) or OpenLogReplicator
    (open-source, modern), or GoldenGate for the enterprise tier.

The adapters all expose **the same `CdcSource` port** — the processor
does not know what database it is reading from.

---

## Testing strategy

Item 6 of the brief. Three layers:

1. **Unit tests.**
   - `ByteToClassParserImplV1` / `V2`: golden-file tests on real
     wal2json output (insert/update/delete/message, transactional and
     not, with/without `include-xids`).
   - `RouterSink`: scheme parsing + fallback behavior.
   - `BackOff` policies: deterministic sequences, jitter bounds.
   - Each `EventSink` adapter against a mocked SDK client (MockK), one
     happy path and one transient failure per adapter.

2. **Integration tests** via Testcontainers (already in
   [`build.gradle.kts:46`](build.gradle.kts:46)). Matrix:

   | Source            | Sink                  |
   |-------------------|-----------------------|
   | Postgres 14 / 17  | LocalStack SNS / SQS  |
   | Postgres 17       | Kafka (Confluent CP)  |
   | Postgres 17       | RabbitMQ              |
   | MySQL 8 (binlog)  | LocalStack SNS / SQS  |
   | MySQL 8 (poller)  | Kafka                 |

   Scenarios per cell: emit N messages, verify ordered delivery, verify
   LSN/GTID checkpoint advanced.

3. **Fault-injection tests** (also Testcontainers, slower lane):
   - Restart the broker container mid-stream; expect retries and zero
     loss.
   - Restart Postgres mid-stream; expect slot recovery and zero
     duplicates beyond the at-least-once contract.
   - Kill the publisher mid-batch; expect resume from the last acked
     LSN.
   - Corrupt payload in the slot; expect dead-letter and continued
     progress.

JaCoCo coverage gate stays in [`config/jacoco.gradle`](config/jacoco.gradle).
Target: 80% line coverage, **100%** on the `application/CdcProcessor`
class.

---

## Build & local dev

```sh
# spin up postgres + localstack
./gradlew startDockerCompose

# build + test
./gradlew build

# stop everything
./gradlew stopDockerCompose
```

`docker-compose.yml` boots `debezium/postgres:14-alpine` and
`localstack/localstack:latest`. LocalStack bootstrap scripts under
[`shell-scripts/localstack/`](shell-scripts/localstack/) create the test
SNS topic and SQS queue.

### Toolchain (post Wave 1)

| Tool          | Version |
|---------------|---------|
| Kotlin        | 1.9.25  |
| JVM target    | 17 (compiled with JDK 21) |
| Gradle wrapper| 8.10.2  |
| Spring (msg)  | 6.0.13 (transitive) |
| AWS SDK v2    | 2.21.1  |
| AWS SDK v1    | 1.12.566 *(legacy, to remove in Wave 2)* |
| pgjdbc        | 42.6.0  |
| HikariCP      | 5.1.0   |
| Micrometer    | 1.12.13 |
| Detekt        | 1.23.7  |
| ktlint plugin | 12.1.1  |
| Testcontainers| 1.19.1  |

---

## Working with Claude / AI agents on this repo

The repo carries a [`CLAUDE.md`](CLAUDE.md) with the engineering rules
that every AI-assisted change must follow, and a Tech Lead persona at
[`.claude/agents/tech-lead.md`](.claude/agents/tech-lead.md) that is
**invoked before every non-trivial commit** to audit the diff against
the user's last instruction. The Tech Lead applies a fixed checklist
(deliverables, hexagonal compliance, delivery guarantees, tests,
operability) and returns a PASS / FAIL verdict. Commits with
BLOCKER- or MAJOR-severity findings must not land on `main`.

---

## License

TBD. The original repository did not ship a `LICENSE` file; one will be
added when the project is open-sourced. Until then this code is private.

## Acknowledgements

Forked from
[`inventa-shop/kotlin-postgres-cdc-to-sns-module`](https://github.com/inventa-shop/kotlin-postgres-cdc-to-sns-module)
(authors: Inventa engineering team, 2023-2024). The roadmap above is the
direction this fork takes.
