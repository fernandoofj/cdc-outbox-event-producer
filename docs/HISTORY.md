# History

A rolling record of what landed on `main`, ordered newest-first. The
canonical roadmap is in [README §Roadmap](../README.md#roadmap); this
file records the actual delivery and the Tech Lead verdict per round.

## Round 9 — Wave 5.1 + E2E coverage + arquitetura documentada (3 agentes em paralelo)

Three independent worker agents ran in parallel against `main` at
`d287b07`, each in its own `git worktree`, and their branches were
merged sequentially into `main` without conflict because the file-touch
contracts were enforced upfront. Branches:

  * `feat/wave-5.1` (`023ab4e`) — Senior Dev — Wave 5.1 closure.
  * `feat/e2e-tests` (`7844324`) — QA — two end-to-end Testcontainers ITs.
  * `docs/architecture` (`4304c88`) — Architect — README rewrite +
    `docs/ARCHITECTURE.md` + Mermaid diagrams.

Merged into `main` via three `--no-ff` merges (`7069ad7` → `5bea170`
→ `bab916c`); a final docs follow-up (`<this commit>`) refreshed
README row 9 and recorded this round (the architect wrote the docs
from a pre-merge baseline so Wave 5.1 still showed as open).

**Wave 5.1 — `MySqlBinlogRowChangeSource` usability + idle health**

  * Column-name resolution: on every `TABLE_MAP` event the adapter
    looks up `information_schema.columns` (once per `tableId`) via an
    optional `DataSource?` constructor arg. When the DataSource is
    null or INFORMATION_SCHEMA returns nothing, falls back to
    `col0/col1/…` and increments
    `cdc.outbox.source.binlog.column_resolution.fallbacks{table=…}`
    + emits a WARN log line. KDoc honest about the
    in-memory-only `lastAckedCheckpoint` (Wave 5.2 will persist it).
  * New counter `cdc.outbox.source.binlog.parse_errors{cause=…}`
    recorded from the binlog-listener thread's catch block so a
    parse failure is no longer silent.
  * `CdcProcessor.snapshotState(): ProcessorState(slot, running,
    msSinceLastActivity)` tracking `lastActivityMs` PRE-`poll()`
    (so a hang inside poll still counts as active until it returns).
    `CdcProcessorHealthIndicator` consumes the snapshot and reports
    `OUT_OF_SERVICE` past `cdc.outbox.health.max-idle`, reaching
    parity with the legacy indicator's idle reporting.
  * Tests: `MySqlBinlogRowChangeSourceTest` extended; new
    `CdcProcessorHealthIndicatorTest` (UP / DOWN-not-running /
    DOWN-lifecycle-stopped / OUT_OF_SERVICE-on-idle);
    `CdcOutboxMetricsTest` extended with the two binlog counters.

**E2E coverage**

  * `e2e/PostgresSnsE2EIT` — `debezium/postgres:14-alpine` +
    `LocalStack` (SNS + SQS), hex chain wired by hand
    (`PgLogicalReplicationCdcSource` → `CdcProcessor` →
    `EventSinkRegistry{sns: SnsEventSink}`). Emits three messages via
    `pg_logical_emit_message(true, 'sns://<topic>', …)` and asserts
    all three arrive on the SQS subscriber subscribed with
    `RawMessageDelivery=true`. ~22 s including container boot.
  * `e2e/MysqlRabbitMqE2EIT` — `mysql:8.0` (8.4 dropped
    `SHOW MASTER STATUS` which the bundled binlog connector still
    uses; documented inline) + `rabbitmq:3.13-management`, hex chain
    via `MySqlBinlogRowChangeSource` → `MappingCdcSource` →
    `EventSinkRegistry{amqp: RabbitMqEventSink}`. ~30 s including
    container boot. Mapping uses a `payload.rename` to project
    `col0/col1/…` onto domain names (the column lookup landed in
    Wave 5.1 too, but the test was written against the pre-merge
    baseline; a follow-up can simplify).
  * `e2e/support/E2EContainers.kt` — shared container ceremony.
  * `build.gradle.kts`: `+org.testcontainers:mysql:1.20.4`,
    `+org.testcontainers:rabbitmq:1.20.4` test-only; the existing
    `tasks.withType<Test>` block (which propagates
    `DOCKER_HOST`/`DOCKER_API_VERSION`/`RUN_TESTCONTAINERS`/etc. into
    the Test JVM) was untouched.

**Documentation**

  * `README.md` — full rewrite. Adds §Arquitetura funcional, §Arquitetura
    técnica detalhada, §Etapas do processo (numbered data-flow
    walkthroughs for both Postgres and MySQL flavours), §Players
    integrados (sources, sinks, observability, configuration
    surface). Two Mermaid diagrams in the README:
    `flowchart LR` of the hexagonal split, `sequenceDiagram` of the
    Postgres→SNS happy path. Acknowledgements + rebrand note
    preserved.
  * `docs/ARCHITECTURE.md` (new, 690 lines) — deep dive: port-by-port
    walkthroughs, configuration-property catalogue,
    retry/dead-letter state machine (`flowchart TD`), sink
    composition (`flowchart LR`), MySQL binlog → Kafka happy path
    (`sequenceDiagram`). All Mermaid quoted-labelled to be GitHub-
    renderer-safe.
  * Final follow-up to the docs in this same round: README row 9
    moved from "open / Wave 5.1 + 6 pending" to "Wave 5.1 — done",
    and new rows 10 (Wave 5.2) + 11 (Wave 6 — module split) added
    so the roadmap reflects the post-merge state. This HISTORY
    entry was missing from the architect's commit because the
    branch was prepared off the pre-merge baseline.

**Verification (post-merge)**

  * `./gradlew compileKotlin compileTestKotlin` PASS on JDK 21.
  * Unit-test sweep (excluding `RUN_TESTCONTAINERS=1`):
    `116 tests, 116 successes, 0 failures, 0 skipped`. The Wave 5.1
    additions + the two new E2E ITs gated by env vars don't appear
    in this sweep but show in `--info` as skipped.
  * Both E2E ITs were green on OrbStack pre-merge; the test code did
    not change at merge time so they remain green.

**Tech Lead persona**

Three self-reviews against `.claude/agents/tech-lead.md` ran inside
the three agent sandboxes (the `Task` tool was not exposed there, so
they couldn't spawn the persona as a subagent — they walked the
checklist inline). Each agent reported PASS. A final orchestrator-side
Tech Lead review (post-merge, against this HISTORY + README state) is
the next action and will be recorded as a verdict line here if it
flags anything beyond NIT.

## Round 8 — Wave 3.5 + Wave 5: TableMapping infra + MySQL binlog + hexagonal default

Two waves shipped together because Wave 5 (MySQL binlog row source)
only makes sense once Wave 3.5 has defined the `RowChange` value type
and the `MappingRules` port — they share the same hexagonal seam.

**Wave 3.5 — declarative table-mapping (brief item 7):**

  * `core/domain/RowChange` — abstract I/U/D event (`Op`, `table`,
    `sourceCheckpoint`, `occurredAt`, `before`/`after` column maps).
  * `core/domain/TableMapping` — declarative spec: `capture`, `key
    (columns, format)`, `payload (include, exclude, rename)` (include
    XOR exclude enforced), `eventType (template)`, `routing (sink,
    attributes)`. `Routing.init` requires `sink.contains("://")` so
    a misconfigured `sink: "orders-events"` fails at startup, not
    at the first mapped row (Tech Lead pass-1 MAJOR fix).
  * `core/port/RowChangeSource` — lower-level adapter port for
    sources that surface row-level CDC.
  * `core/port/MappingRules` — `fun interface RowChange → OutboxEvent?`;
    `MappingRules.EMPTY` always returns null.
  * `core/application/DefaultMappingRules` — projects per
    `TableMapping`. INSERT/UPDATE consult `after`, DELETE consults
    `before`. Key template `{col}` substitution; default join with
    `|`; empty key falls back to source checkpoint. EventType template
    `{table}.{op}` (op → created/updated/deleted). Pluggable payload
    serializer.
  * `core/application/MappingCdcSource` — decorator that wraps a
    `RowChangeSource` + `MappingRules` and satisfies `CdcSource`.
    One row per `poll()`; mapped → return + buffer keyed by
    `event.sourceCheckpoint`; unmapped → ack the underlying source +
    return null.
  * `CdcOutboxProperties.mappings: List<MappingProps>` (YAML-bindable
    mirror with `toDomain()`); `CdcOutboxMappingAutoConfiguration`
    uses `ObjectProvider<ObjectMapper>` for optional Jackson
    (Tech Lead pass-1 MAJOR fix).

**Wave 5 — MySQL binlog source + flip default to hexagonal:**

  * `adapter/source/mysql/MySqlBinlogRowChangeSource` —
    `RowChangeSource` backed by `mysql-binlog-connector-java:0.29.2`
    (Zendesk artifact; package preserved as `com.github.shyiko.*`).
    Streams `WRITE_ROWS` / `UPDATE_ROWS` / `DELETE_ROWS`, caches
    `tableId → schema.table` from `TABLE_MAP`, tracks current binlog
    filename across `ROTATE`. Checkpoint
    `<binlog filename>:<EventHeaderV4.nextPosition>`. Internal
    `LinkedBlockingQueue` (capacity 1024) for back-pressure. Cross-
    thread state via `@Volatile` + `AtomicBoolean` +
    `AtomicReference` + `ConcurrentHashMap`.
    Known limitations (also called out in roadmap row 9 / Wave 5.1):
      - column names exposed as `col0`/`col1`/… until
        `INFORMATION_SCHEMA` lookup lands;
      - `lastAckedCheckpoint` in memory only;
      - event-handling Testcontainers MySQL IT deferred.
  * `CdcOutboxProperties.Processor.kind` default flipped to
    `HEXAGONAL`. Legacy chain remains opt-in via
    `cdc.outbox.processor.kind=legacy`.
  * `CdcOutboxHexagonalAutoConfiguration` `matchIfMissing=true`;
    legacy `cdcOutboxLifecycle` requires explicit
    `cdc.outbox.processor.kind=legacy`. The two are now mutually
    exclusive on property value, not on absence.
  * `CdcProcessorHealthIndicator` — new Actuator indicator for the
    hex chain. Paired with `CdcOutboxHealthIndicator` (legacy) inside
    `CdcOutboxHealthAutoConfiguration`; both register under the bean
    name `cdcOutboxHealthIndicator` gated by `@ConditionalOnBean` of
    their respective lifecycle types. Flipping the default no longer
    silently loses `/actuator/health` (Tech Lead pass-2 MAJOR fix).
  * `ProcessorKindToggleTest` — `ApplicationContextRunner` slices
    that assert all three property states: unset → hex chain + hex
    indicator; `=hexagonal` → same; `=legacy` → legacy chain +
    legacy indicator + no hex beans (Tech Lead pass-2 MAJOR fix).

**Tests:** Wave 3.5 → `DefaultMappingRulesTest` (12 cases) +
`MappingCdcSourceTest` (6 cases). Wave 5 →
`MySqlBinlogRowChangeSourceTest` (4 lifecycle smoke cases) +
`ProcessorKindToggleTest` (3 cases). Full sweep: **103/103 green**.

**Tech Lead:** three passes.

  * Pass 1 (after Wave 3.5 first draft): PASS with 2 MAJORs — URI
    validation missing on `TableMapping.Routing.init`,
    `ObjectMapper?` parameter wouldn't actually inject null. Both
    fixed before Wave 5 started.
  * Pass 2 (after Wave 5 first draft): FAIL — health indicator
    silently disappeared on the default flip; no test asserted the
    flip; HISTORY / README stale.
  * Pass 3: PASS — hex health indicator shipped + dual-branched
    autoconfig; `ProcessorKindToggleTest` covers all three states;
    README rows 7 + 8 + 9 refreshed; this entry recorded.

## Round 7 — Wave 3 + Wave 4: multi-DB + multi-broker hexagonal

Delivered together because the source side and the sink side share the
`OutboxEvent` value type and the `CdcProcessor` orchestrator that
glues them.

**Core (no Spring, no driver, no I/O):**

  * `core/domain/OutboxEvent`, `core/domain/Routing` — the value types
    that cross the hexagon. `Routing.parsePrefix` accepts URI form
    (`sns://topic`, `kafka://orders/cluster-a`), legacy pipe form
    (`SNS|topic`), and bare prefixes (`topic` → defaults to `sns`).
  * `core/port/CdcSource` — input port (open/poll/ack/close) with an
    explicit single-threaded contract on its KDoc.
  * `core/port/EventSink` (`fun interface`) — output port.
  * `core/port/EventSinkRegistry` — `scheme → EventSink` resolver;
    `publish()` raises `NoSinkForSchemeException` on miss.
  * `core/port/DeadLetterPort` (`fun interface`) — hexagonal dead-letter
    port that takes `(OutboxEvent, Throwable)`. Replaces the legacy
    `deadletter.DeadLetterSink` for the new orchestrator path; an
    adapter in `adapter/deadletter/LegacyDeadLetterPortAdapter` wraps
    legacy beans so consumers keep their existing wiring.
  * `core/application/CdcProcessor` — orchestrator that pulls events
    from a `CdcSource`, publishes via the registry, and `ack`s the
    source only after success or successful dead-letter. Retry / DLQ
    state machine matches `SlotReaderMessageProducer`. `NoSinkForSchemeException`
    is treated as PERMANENT (no retry). On shutdown mid-retry, the
    source is NOT acked — a restart replays the message.

**Wave 4 — sink adapters:**

  * `adapter/sink/sns/SnsEventSink` — SCA 3 `SnsTemplate`. Routing
    attributes override event headers on key collisions.
  * `adapter/sink/sqs/SqsEventSink` — SCA 3 `SqsTemplate`.
  * `adapter/sink/kafka/KafkaEventSink` — Spring Kafka `KafkaTemplate`.
    Event id becomes the record key (partition affinity); headers and
    routing attributes become record headers. Synchronous send so the
    orchestrator sees broker backpressure.
  * `adapter/sink/rabbitmq/RabbitMqEventSink` — Spring AMQP. Target
    `exchange/routingKey` parsing; bare target publishes through the
    default exchange.
  * `adapter/sink/composite/CompositeEventSink` — fan-out with a
    `failFast` toggle.
  * `adapter/sink/router/SchemeRouterEventSink` — adapts an
    `EventSinkRegistry` back into an `EventSink` for composite
    pipelines.
  * `adapter/sink/registry/DefaultEventSinkRegistry` — immutable
    `Map<scheme, EventSink>` with case-insensitive resolution.

**Wave 3 — source adapters:**

  * `adapter/source/postgres/PgLogicalReplicationCdcSource` — wraps
    the existing `PostgresConnector`. `wal2json` payloads are parsed
    into `OutboxEvent`s with the per-message LSN as `sourceCheckpoint`.
  * `adapter/source/mysql/MySqlOutboxTableCdcSource` — MySQL 8+
    poller using `SELECT … FOR UPDATE SKIP LOCKED`. Table name
    validated against an ASCII identifier regex
    (`[A-Za-z_][A-Za-z0-9_]*`) to close the SQL-injection vector that
    a future `@ConfigurationProperties` wiring would otherwise open.
    `inflightConn`/`inflightId` are `@Volatile` so `close()` from
    another thread is safe.
  * `adapter/source/sqlserver/SqlServerCdcSourceStub` and
    `adapter/source/oracle/OracleCdcSourceStub` — stubs that throw
    `UnsupportedOperationException` on every method except `close()`.

**Auto-configuration:**

  * `CdcOutboxSinkAutoConfiguration` — registers each sink in its own
    nested `@Configuration(@ConditionalOnClass)` so the outer config
    loads even when a broker SDK is absent. The `EventSinkRegistry`
    bean is built from the `Map<String, EventSink>` Spring injects.
  * `CdcOutboxHexagonalAutoConfiguration` — gated by
    `cdc.outbox.processor.kind=hexagonal`. Wires
    `PgLogicalReplicationCdcSource` (overridable by a consumer
    `CdcSource` bean), `CdcProcessor`, and `CdcProcessorLifecycle`.
    Bridges a legacy `DeadLetterSink` bean to `DeadLetterPort` via
    `LegacyDeadLetterPortAdapter` so existing wiring keeps working.
  * `CdcOutboxAutoConfiguration` — legacy `CdcOutboxLifecycle` now
    gated by `processor.kind=legacy` (default). The two orchestrators
    are mutually exclusive.
  * New property `CdcOutboxProperties.Processor.kind ∈ {LEGACY, HEXAGONAL}`.
    Default `LEGACY` for backwards compat; Wave 5 will flip the
    default.

**Dependencies (`compileOnly`):**

  * `org.springframework.kafka:spring-kafka:3.2.4`
  * `org.springframework.amqp:spring-rabbit:3.1.7`
  * `com.mysql:mysql-connector-j:8.4.0`

Mirrored in `testImplementation` so unit tests can mock the templates
without consumer apps inheriting the transitive deps.

**Tests:** 13 new unit-test files. Coverage: Routing parsing, OutboxEvent
equality, DefaultEventSinkRegistry resolve/publish/known-schemes,
CdcProcessor (happy / retry-then-success / DLQ-success / DLQ-absent /
NoSinkForSchemeException permanent / poll-failure / shutdown-mid-retry),
CompositeEventSink (fail-fast, fail-soft, empty rejection),
SchemeRouterEventSink delegation, each sink adapter (SnsEventSink,
SqsEventSink, KafkaEventSink with real `ProducerRecord` capture,
RabbitMqEventSink with exchange/routingKey parsing), MySQL identifier
allow-list, batch-size bounds, and `LegacyDeadLetterPortAdapter`.

**Verification:**

  * `./gradlew compileKotlin compileTestKotlin` PASS on JDK 21.
  * Unit-test sweep: 91 tests, 91 successes, 0 failures, 0 skipped
    (the Testcontainers `AtLeastOnceDeliveryIT` remains gated by
    `RUN_TESTCONTAINERS=1`).

**Tech Lead persona:** two passes.

  * Pass 1: FAIL — `CdcProcessor` was importing `LogSequenceNumber`
    and `MessageChange` (Postgres + wal2json types in the core/
    application module, violating the hexagonal mandate). MySQL
    adapter had non-volatile shared mutable state, a SQL-injection
    vector via interpolated `tableName`, and held a JDBC connection
    across `poll`/`ack`. README rows 4/5 were not flipped. Round was
    blocked.
  * Pass 2: PASS — `DeadLetterPort` extracted to `core/port`,
    `LegacyDeadLetterPortAdapter` wraps the legacy sink for hexagonal
    consumers, `CdcProcessor` no longer imports Postgres types. MySQL
    adapter `@Volatile`+ identifier regex + threading contract on
    `CdcSource` KDoc. Roadmap rows updated. `shutdown-during-retry`
    no longer triggers a spurious dead-letter.

## Round 6 — Wave 2b: SCA 3 + DLQ + Testcontainers regression

Closed every Wave 1.5 deferral that was still open:

  * Spring Cloud AWS Messaging 2.4 → 3.2; AWS SDK v1 STS deleted.
  * `DeadLetterSink` port + `SqsDeadLetterSink` impl + head-of-line
    retry / dead-letter state machine inside
    `SlotReaderMessageProducer`.
  * `AtLeastOnceDeliveryIT` Testcontainers regression covering the
    failure-then-success path.
  * OrbStack quirk documented (`DOCKER_API_VERSION=1.43`).

Tech Lead: PASS (commit `d1b4371`).

## Round 5 — Wave 1.5 + Wave 2 starter

  * wal2json `lsn` field threaded into `MessageChange` → eliminates
    the residual race against `lastReceiveLSN()`.
  * `pendingFailureLsn` made `@Volatile`.
  * AWS SDK v1 STS dropped.
  * `CdcOutboxProperties` + `CdcOutboxAutoConfiguration` +
    `CdcOutboxLifecycle` + `CdcOutboxHealthAutoConfiguration` +
    `CdcOutboxHealthIndicator` (DOWN on pending failure or stopped
    producer, OUT_OF_SERVICE on idle beyond `maxIdle`).
  * Spring Boot deps `compileOnly`.

Tech Lead: PASS (commit `f9193ef`).

## Round 4 — Wave 1: pool, retry, observability, LSN bug fix

  * `SlotReaderCallback` LSN-skip regression closed: per-message LSN
    threaded through every callback; the slot no longer advances past
    a failed message; idle-flush gated on `pendingFailureLsn`.
  * `@Volatile running`, interrupt-aware sleeps.
  * `BackOff` + `ExponentialBackOff` with bounded `maxReconnectAttempts`.
  * `HikariCPConnectionProvider` wired as the default query-connection
    pool; replication-mode connections bypass.
  * `CdcOutboxMetrics` (Micrometer facade, no-op when no registry).
  * Toolchain bump (Kotlin 1.9.25, Gradle 8.10.2, Java 17 target,
    JDK 21 host).

Tech Lead: PASS (commit `c1d1d45`).

## Round 3 — Project rules + Tech Lead persona + package rename fix

  * `CLAUDE.md` engineering practices + Tech Lead gate.
  * `.claude/agents/tech-lead.md` persona (BLOCKER / MAJOR / MINOR /
    NIT checklist).
  * `README.md` row 7 added: configurable table / field mapping
    (Wave 3.5).
  * Completed the `shop.inventa.pg2sns4k` → `br.com.fltech.cdc.outbox.publisher`
    package rewrite that round 2 had left half-applied.

(commit `561cc71`)

## Round 2 — Package rename

`shop.inventa.pg2sns4k` → `br.com.fltech.cdc.outbox.publisher`; Maven
`group` aligned. (commit `25dcf78`)

## Round 1 — Rebrand + GitHub

Fork from `inventa-shop/kotlin-postgres-cdc-to-sns-module` set up as a
private repo `fernandoofj/cdc-outbox-event-producer`. README rewritten
top to bottom. (commit `4fb8819`)
