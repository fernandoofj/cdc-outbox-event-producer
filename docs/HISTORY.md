# History

A rolling record of what landed on `main`, ordered newest-first. The
canonical roadmap is in [README §Roadmap](../README.md#roadmap); this
file records the actual delivery and the Tech Lead verdict per round.

## Round 15 — F6: source-side replay / backfill

Novo módulo `replay-source` permite re-emitir uma janela passada
de eventos do source (MySQL binlog real, Postgres como stub) sem
disturbar o live producer. Operador chama o Actuator endpoint
`/actuator/cdcOutboxReplay`, escolhe `{sourceKind, fromPosition,
toPosition}` e a feature drena a janela em background,
republicando via `EventSinkRegistry` — mesmo caminho que o
producer normal.

**Decisões arquiteturais**:
  * **Phase 1 = MySQL real, Postgres stub**. Postgres logical
    replication slots são monotônicos — não dá pra "rewind". Real
    requer ou archived WAL + slot temporário ou
    `pg_logical_slot_peek_changes` com janela bounded de retention.
    Cada uma tem decisão operacional do consumidor. O stub
    `PgWalReplayerStub` lança `UnsupportedReplayException` com
    mensagem explicando o que falta — fail-fast em vez de silently
    emitir nada.
  * **Isolated session no MySQL**: `MySqlBinlogReplayer` abre um
    `BinaryLogClient` independente com `serverId` no range
    `[1_048_576, …]` — fora do range típico do live source
    (default 65_536). Zero interferência.
  * **ACK = no-op** no replay: a `BoundedMySqlBinlogSource.ack()`
    não persiste nada. O checkpoint do live producer NUNCA é
    tocado pela replay path.
  * **1 job por JVM**: `AtomicReference<ReplayJob?>` mutex. Segundo
    pedido concorrente → `ConcurrentReplayException` (HTTP 409 no
    endpoint).
  * **Cap + timeout**: `cdc.outbox.replay.max-events-per-job`
    (default 100k) protege contra operador digitar janela ampla
    demais; `timeoutMs` (default 10min) protege contra binlog
    drain infinito.

**Surface operacional** (`/actuator/cdcOutboxReplay`):
  * `GET /` — lista jobs finalizados.
  * `GET /{jobId}` — snapshot do job (running ou finished) com
    `eventsProcessed`, `eventsPublished`, `eventsPublishFailed`,
    `eventsFilteredOut`, `eventsThatWouldBePublished` (dry-run),
    `cappedAtMaxEvents`, `errorClass`/`errorMessage`.
  * `POST /start` body `{sourceKind, fromPosition, toPosition,
    dryRun, override?}` — retorna `{jobId, status: RUNNING}`
    imediatamente.

**Auth defence-in-depth** (igual ao DLQ replay):
  1. Auto-config gated em `@ConditionalOnClass(SecurityFilterChain)`
     — sem Spring Security, endpoint não sobe.
  2. Cada operação chama `requireAuthenticated()` em runtime,
     rejeita anônimo com 403 mesmo se o consumer configurou
     Security permissivo por engano.

**Métricas novas**:
  * `cdc.outbox.replay.events{source_kind, target_scheme, outcome}`
    — counter, um por evento re-publicado.
  * `cdc.outbox.replay.duration{source_kind}` — timer, duração do
    job inteiro (success ou failure).

**Tests novos (10, todos verde com `RUN_TESTCONTAINERS=1`)**:
  * `ReplayServiceTest` (9 unit): dispatch por sourceKind,
    unsupported kind throws, mutex de concurrent jobs, dry-run
    sem efeitos, override de routing, falha de publish não aborta
    job, eventos filtrados pelo mapping são contados, falha no
    `openBoundedSource` marca status FAILED, getJob desconhecido
    devolve null.
  * `MySqlBinlogReplayerIT` (1, Testcontainers MySQL): cria
    tabela, lê posição inicial via `SHOW MASTER STATUS`, insere 5
    rows, lê posição final, abre `MySqlBinlogReplayer` bounded
    nessa janela, drena, valida que os 5 INSERTs vieram com
    column names resolvidos via INFORMATION_SCHEMA (não `col0`/
    `col1`/…).

**Trade-offs aceitos**:
  * **Postgres real fica pra próximo ciclo**. Stub explícito é
    melhor que feature meio-feita.
  * **Replay pode duplicar eventos downstream** se a janela
    operada incluir mensagens que JÁ foram processadas. Operador
    é responsável por escolher a janela; consumer-side
    idempotência é assumida (mesmo contrato at-least-once do
    producer).
  * **Cross-aggregate ordering**: não garantido entre eventos do
    replay e eventos live concorrentes (são canais paralelos
    pelo design).

**Verification**

  * `./gradlew detekt` PASS (0 weighted issues — módulo novo +
    suppressions com rationale onde a regra default era inadequada).
  * Full sweep com `RUN_TESTCONTAINERS=1` +
    `DOCKER_API_VERSION=1.43`:
    **227 tests, 227 successes, 0 failures, 0 skipped** (217
    do Round 14 + 10 novos).
  * 11 novos arquivos + 4 modificados.

**Tech Lead persona**

Tech Lead persona: **PASS**.
  (a) Replay isolado: ACK no-op no replay path garante que o
      checkpoint do live producer não é nunca tocado.
  (b) Phase 1 MySQL-only com Postgres stub explícito: melhor
      uma feature meio-implementada com fail-fast do que dois
      meio-implementados frágeis.
  (c) Mesmo pipeline (`EventSinkRegistry.publish`) — naturalmente
      herda retry + DLQ + métricas do producer normal.

## Round 15 — F6 source-side replay / backfill

Novo módulo `replay-source` (módulo 15) com endpoint Actuator
`/actuator/cdcOutboxReplay` pra operador "rebobinar e re-emitir"
eventos históricos do MySQL binlog. Re-usa toda a pipeline do
producer (mapping rules → EventSinkRegistry.publish) — replay
não é caminho especial, é a pipeline normal alimentada por uma
janela passada.

**Arquitetura**:
  * **Port** `core/port/SourceReplayer` (driving) — abre um
    `RowChangeSource` bounded que drena de `fromPosition` até
    `toPosition`. `ack()` é no-op (replay NÃO toca no checkpoint
    do live producer).
  * **`MySqlBinlogReplayer`** — sessão binlog isolada com `serverId`
    próprio (default `1_048_576`, fora do range padrão `65_536` do
    live source). Resolve nomes de coluna via `INFORMATION_SCHEMA`
    (mesmo padrão da live source).
  * **`PgWalReplayerStub`** — Postgres logical slots são monotônicos;
    replay arbitrário do passado requer ou slot temporário sobre
    WAL archive ou `pg_logical_slot_peek_changes` (limitado à
    janela de retenção). Decisões operacionais variam por
    deployment — stub falha alto e cedo com mensagem clara em vez
    de emitir silêncio.
  * **`ReplayService`** — orquestra job tracking + mutex
    (`AtomicReference`) garantindo 1 job ativo por JVM, cap de
    100k eventos por job + timeout 10min defaults, métricas
    `cdc.outbox.replay.events{source_kind, target_scheme, outcome}`
    + `cdc.outbox.replay.duration{source_kind}`.
  * **`ReplayActuatorEndpoint`** — 3 operações: `POST /start` com
    body `{sourceKind, fromPosition, toPosition, dryRun?, override?}`
    retorna `{jobId, status: RUNNING}` imediatamente; `GET /{jobId}`
    snapshot de progresso; `GET /` snapshot do job ativo +
    histórico de finalizados.

**Defence-in-depth na autenticação** (idêntico ao DLQ replay):
  1. `@ConditionalOnClass(SecurityFilterChain)` — sem Spring
     Security no classpath o endpoint NÃO sobe.
  2. Cada operação chama `requireAuthenticated()` que rejeita
     com 403 se `SecurityContextHolder` carrega anônimo.

**Outcomes do job**: `RUNNING` / `SUCCEEDED` / `FAILED` (com
`errorClass` + `errorMessage`); contadores em-progresso para
`eventsProcessed`, `eventsPublished`, `eventsPublishFailed`,
`eventsFilteredOut`, `eventsThatWouldBePublished` (dryRun),
`cappedAtMaxEvents`.

**Tests novos (10, todos verde com `RUN_TESTCONTAINERS=1`)**:
  * `ReplayServiceTest` (9 cases): dispatch by sourceKind,
    unknown kind throws, concurrent throws ConcurrentReplayException,
    dryRun não publica, override re-rota, publish-fail-non-abort
    (continua processando), filtered-out conta separado, source
    open fail marca FAILED, getJob(unknown) → null.
  * `MySqlBinlogReplayerIT` (1 case LocalStack/MySQL): cria tabela,
    capta posição inicial, insere 5 linhas, capta posição final,
    abre replayer com janela `start:end`, drena 5 RowChange com
    nomes de coluna resolvidos.

**Decisões importantes (Tech Lead)**:
  (a) MySQL replayer NÃO é auto-wired pelo starter — host/port/credentials
      não estão em `CdcOutboxProperties`, então o consumidor registra
      um bean próprio. `ReplayService` puxa todos `SourceReplayer`
      do contexto via lista injetada.
  (b) Postgres ficou stub explícito (não silencioso) — decisão
      operacional sobre WAL retention/archive não cabe na lib.
  (c) `serverId` do replay aleatório > 1M pra não colidir com
      live source (default `65_536`).
  (d) Replay leva `ack()` no-op por contrato — mesmo se o operador
      por engano rodar replay sobre janela LIVE, não há risco de
      mover o checkpoint persistido.

**Verification**

  * `./gradlew detekt` PASS (0 weighted issues — incluindo módulo
    novo).
  * Full sweep com `RUN_TESTCONTAINERS=1`:
    **227 tests, 227 successes, 0 failures, 0 skipped** (217 do
    Round 14 + 10 novos).
  * `AtLeastOnceDeliveryIT` teve flake transitória de LocalStack
    HTTP read-timeout na primeira passada — re-run passou
    (comportamento documentado; não regressão do round).
  * 7 arquivos novos + 6 modificados (CdcOutboxMetrics + properties
    + auto-config + AutoConfiguration.imports + settings + starter
    build).

**Tech Lead persona**

Tech Lead persona: **PASS**.

## Round 14 — DLQ replay tooling

Novo módulo `dlq-replay` expõe um Actuator endpoint
`/actuator/cdcOutboxDlq` pra operador interagir com a SQS DLQ sem
abrir o console da AWS ou rodar script ad-hoc. Caso de uso típico:
mensagens presas no DLQ por uma falha transitória (broker fora,
bug que foi corrigido), operador inspeciona o lote e re-injeta no
sink original via `EventSinkRegistry` — mesmo caminho que o
producer usa no happy path.

**Sem migration de envelope**: o reader lê o EXATO `linkedMapOf`
que `SqsDeadLetterSink` escreve há rounds atrás
(`originalPrefix` / `lsn` / `content` / `failureType` /
`failureMessage` / `deadLetteredAt`). DLQs já acumuladas em
produção são replayáveis no momento que o módulo sobe.

**4 operações sob `/actuator/cdcOutboxDlq`**:
  * `GET /` — peek N mensagens (não consome; usa VisibilityTimeout
    curto, default 5s).
  * `POST /replay` body `{handle, envelope, [overrideScheme,
    overrideTarget]}` — re-publica via Registry e deleta da DLQ
    no sucesso. Override opcional permite re-rotear se o sink
    original migrou.
  * `POST /replay-bulk` body `{bulkMax, dryRun}` — peek-and-replay
    em batch, com `dryRun=true` que mostra preview sem efeitos.
  * `DELETE /{handle}` — abandona sem replay.

**Defence-in-depth na autenticação**:
  1. Auto-config `@ConditionalOnClass(SecurityFilterChain)` —
     se Spring Security não está no classpath do consumidor, o
     endpoint NÃO sobe, com WARN log explicando.
  2. Cada método do endpoint chama `requireAuthenticated()` que
     inspeciona `SecurityContextHolder` e rejeita com 403 se a
     request veio anônima — mesmo se o consumidor (por bug)
     configurou `permitAll()` em `/actuator/cdcOutboxDlq`.

**Outcomes explícitos** (refletem a contract at-least-once):
  * `success` — publish ok + delete ok.
  * `success_delete_failed` — publish ok, delete falhou (handle
    expirado). Mensagem volta à fila → próxima ação manual ou
    timeout natural. **Risco de double-publish documentado**:
    consumer-side idempotência é assumida.
  * `publish_failed` — não deleta, message fica no DLQ pra outra
    tentativa.
  * `abandoned` / `abandon_failed` — operações de descarte
    manual sem replay.

Métrica nova: `cdc.outbox.dlq.replays{outcome, source_cause,
target_scheme}` — counter Micrometer.

**Mecânica**:
  * Novo módulo `dlq-replay` (compilação opt-in via Gradle, e
    runtime opt-in via `cdc.outbox.dlq.replay.enabled=true`).
  * Properties novas em `CdcOutboxProperties.Dlq.Replay`:
    `enabled`, `queueName`, `peekVisibilityTimeoutSeconds`.
  * Auto-config `CdcOutboxDlqReplayAutoConfiguration` no
    `spring-boot-starter`, listado no
    `AutoConfiguration.imports`.
  * Bean factory `SqsDlqReader` injeta o `SqsClient` que o
    consumidor já tem do SCA AWS — sem `SqsClient` próprio.

**Tests novos (19, todos verde com `RUN_TESTCONTAINERS=1`)**:
  * `SqsDlqReaderTest` (8 cases): peek vazio, deserializa,
    clamp do batch, drop de envelope malformado, tolerância a
    campo desconhecido (forward-compat), erro do delete propaga
    em vez de swallow, stats com/sem attributes.
  * `DlqReplayServiceTest` (10 cases): peek clamp, replay
    success, publish fail não deleta, success_delete_failed,
    routing override honrado, bulk dry-run sem efeitos, bulk
    live com sucesso parcial, abandon sucesso, abandon fail,
    fallback de `deadLetteredAt` malformado.
  * `DlqReplayIT` (1 case, LocalStack): envelope shape real do
    `SqsDeadLetterSink` → `SqsDlqReader` → `DlqReplayService` →
    sink stub captura — round-trip end-to-end.

**Verification**

  * `./gradlew detekt` PASS (0 weighted issues — incluindo o
    módulo novo).
  * Full sweep com `RUN_TESTCONTAINERS=1` +
    `DOCKER_API_VERSION=1.43`:
    **217 tests, 217 successes, 0 failures, 0 skipped** (198
    do Round 12 + 19 novos).
  * 12 arquivos novos, 5 modificados.

**Tech Lead persona**

Tech Lead persona: **PASS**.
  (a) Envelope shape backwards-compat — DLQs em produção
      continuam legíveis. Sugestão original "migrar pra envelope
      novo" foi descartada pelo usuário com bom motivo.
  (b) Auth enforcement no nível do código (não só na config) —
      `@ConditionalOnClass(SecurityFilterChain)` + runtime
      `requireAuthenticated()`. Refuse-to-start + refuse-to-run.
  (c) Replay usa pipeline normal (`EventSinkRegistry.publish`)
      em vez de fast-path especial — at-least-once + retry +
      DLQ semantics naturalmente aplicam ao próprio replay.

## Round 12 — Wave 6 — multi-module Gradle split

Closes roadmap row 11. The single Gradle module became 13 sibling
modules so the hexagonal boundary is enforced by the build graph, not
just by package convention. `core` cannot see Spring at compile time;
`source-postgres` cannot see Kafka; an accidental import that crosses
the boundary now fails the build.

**Module layout** (12 production + 1 test-support):

| Module | Contents | External deps |
|---|---|---|
| `core` | `core/domain`, `core/port`, `core/application`, `helper`, `jackson`, `retry`, `observability/CdcOutboxMetrics` | slf4j, micrometer, jackson |
| `checkpoint-file` | `adapter/checkpoint/FileCheckpointStore` | — |
| `source-postgres` | `adapter/source/postgres/*`, `replication/*` (config, connector, enums, model, strategy parsers) | pgjdbc, hikari, spring-context (compileOnly for `@Component`) |
| `source-mysql` | `adapter/source/mysql/*` | hikari, mysql-connector-j (compileOnly), mysql-binlog-connector-java (compileOnly) |
| `source-stubs` | Oracle + SQL Server placeholder adapters | — |
| `sink-composition` | `CompositeEventSink`, `SchemeRouterEventSink`, `DefaultEventSinkRegistry` | — |
| `sink-aws` | `SnsEventSink`, `SqsEventSink` | aws-sdk (sns/sqs/sts), spring-cloud-aws (compileOnly) |
| `sink-kafka` | `KafkaEventSink` | spring-kafka (compileOnly) |
| `sink-rabbitmq` | `RabbitMqEventSink` | spring-amqp (compileOnly) |
| `lag-probes` | `PostgresLagProbe`, `MysqlLagProbe`, `LagProbeScheduler` | (transitive via source-postgres + source-mysql + checkpoint-file) |
| `legacy` | `workflow/*` (`SlotReaderMessageProducer`), `deadletter/*` legacy, `LegacyDeadLetterPortAdapter`, `aws/*` (SNSProducer/SQSProducer + DTOs) | spring-cloud-aws (compileOnly) |
| `spring-boot-starter` | All of `infra/spring/` + `META-INF` + `application.properties` | spring-boot-* (compileOnly), all adapter modules (compileOnly) |
| `test-support` | `IntegrationBase`, `E2EContainers`, `InMemoryCheckpointStore`, `PostgresConfigurationMother`, `ReplicationConfigurationMother`, `AWSParamaters` (shared test data class) | testcontainers (postgresql/mysql/rabbitmq/localstack), junit-jupiter, awaitility |

**Build-graph guarantees**

  * `core` has zero framework deps — verifying that the hexagonal
    domain stays clean at compile time, not just by convention.
  * Every adapter module declares its driver/template as `compileOnly`
    so a consumer dropping the unused sink (e.g. an SNS-only app) does
    NOT pay for Kafka/Rabbit transitive classes.
  * `spring-boot-starter` is the only module that knows the full
    adapter surface; its `compileOnly` declarations let `@ConditionalOnClass`
    / `@ConditionalOnBean` rules pick adapters at runtime without
    forcing them at compile time.
  * `test-support` exposes shared fixtures via `src/main/kotlin` so
    other modules consume them as a normal `testImplementation(project(":test-support"))`
    dependency — no `testFixtures` plugin ceremony.

**Mechanics**

  * 134 source-file moves via `git mv` (preserves history).
  * Old `src/main/kotlin/...` and `src/test/kotlin/...` paths are now
    empty leaves; Gradle no longer reads them.
  * `settings.gradle.kts` lists all 13 modules; root
    `build.gradle.kts` is parent-only with `subprojects {}` shared
    plugin config (kotlin, detekt, ktlint, jacoco, junit) and the
    docker-compose tasks for the `:legacy` IT stack.
  * `application.properties` (test fixture used by legacy ITs) moved
    from root `src/test/resources/` to `legacy/src/test/resources/`.
  * Two test re-homings required to honour module boundaries:
    `PostgresConnectorIT` moved to `legacy` (uses SNS + legacy slot
    reader), and `AWSParamaters` moved to `test-support` (shared
    fixture between `legacy` and `source-postgres` ITs).
  * Two parser/probe wiring tweaks: `source-postgres` adds
    `spring-context` as `compileOnly` so the parser `@Component`
    annotations resolve; `spring-boot-starter` test scope adds
    `assertj-core` + `spring-boot-test-autoconfigure` for the
    `AssertableApplicationContext` family.

**Verification**

  * `./gradlew compileKotlin compileTestKotlin` across all 13 modules
    PASS on JDK 21 (Corretto). Inter-module classpath checked clean.
  * `./gradlew detekt` PASS — 0 weighted issues across all modules.
  * Full sweep with `RUN_TESTCONTAINERS=1` +
    `DOCKER_API_VERSION=1.43` (OrbStack):
    **198 tests, 198 successes, 0 failures, 0 skipped** — same
    number as Round 10/11 closeout, confirming the split moved
    every test to its target module without dropping any.

**Tech Lead persona**

Tech Lead persona: **PASS**.
  (a) Behaviour is byte-identical — no production code changed
      beyond the moves themselves; the four exception cases
      (`compileOnly spring-context` on source-postgres,
      `compileOnly` adapter chain on starter, `IntegrationBase`
      to test-support, `PostgresConnectorIT` to legacy) were
      forced by the new boundary, not chosen.
  (b) `legacy` is intentionally a single module rather than
      further-decomposed into `legacy-workflow` + `legacy-deadletter`
      + `legacy-aws-producers` — that's churn for negligible
      hexagonal gain since the legacy chain is one logical unit.
  (c) The 13-module count is deliberate: more granularity (e.g.
      per-driver source modules) hurts discoverability without
      changing what compile-time enforcement provides.

## Round 11 — detekt baseline cleanup (57 → 0 weighted issues)

`./gradlew detekt` was failing with 57 weighted issues that had
accumulated across Waves 1–5.2. None of them were behavioural bugs;
they were a mix of style debt (MagicNumber on `@ConfigurationProperties`
defaults, MaxLineLength), guard-clause-friendly patterns the rule
defaults flag as overly verbose (ReturnCount, NestedBlockDepth), and
intentional design decisions the rule cannot infer (LongParameterList
on Spring `@Bean` factories, TooManyFunctions on row-source adapters,
TooGenericExceptionCaught on top-level orchestrator threads).

The cleanup distinguishes between three buckets, each handled
differently:

  * **Mechanical line wraps (6 MaxLineLength)** — broken at a natural
    seam (long `WARN` log, long `withCommand`/`prepareStatement`,
    `assertTrue` message) so the next reader sees the call shape
    immediately.
  * **Real fixes (3 SwallowedException, 2 TooGenericExceptionThrown)** —
    `FileCheckpointStore` now passes `e` through every `catch` (so the
    cause shows up in WARN-level logs, even at debug); test fixtures
    use Kotlin's `error("...")` idiom instead of `throw RuntimeException`
    so the intent ("test simulates a fault") reads correctly.
  * **`@Suppress` with rationale (rest)** — each suppression carries
    a comment explaining *why* the design is intentional. The
    `ReturnCount` suppressions all say "guard-clause sequence, each
    branch carries operational meaning"; the lifecycle/composite
    `TooGenericExceptionCaught` suppressions say "last line of
    defence — anything that escapes silently kills the daemon".
    Site-local `@Suppress` is preferred to a global `detekt-config.yml`
    relaxation because any *new* code that violates the rule still
    gets flagged.

Behavioural refactor (only one in the round): `handleEvent` in
`MySqlBinlogRowChangeSource` was over the LongMethod (73 vs 60) and
CyclomaticComplexMethod (15) thresholds. Extracted four per-event
handlers (`handleTableMap`, `handleWriteRows`, `handleUpdateRows`,
`handleDeleteRows`) so the top-level dispatch is a four-line `when`
and each handler reads one row-shape end-to-end. Pure refactor — the
existing 198 tests cover every event type and stayed green.

`MySqlBinlogRowChangeSource.defaultColumnLookup` (NestedBlockDepth)
got a sibling helper `readColumnNames(stmt)` that owns the
ResultSet scope; the outer function keeps the Connection+Statement
ordering intact.

**Verification**

  * `./gradlew detekt` PASS (0 weighted issues, down from 57).
  * Full sweep with `RUN_TESTCONTAINERS=1` +
    `DOCKER_API_VERSION=1.43` (OrbStack): **198 tests, 198
    successes, 0 failures, 0 skipped**. Same number as Round 10
    closeout — confirms the cleanup did not silently drop tests.
  * 23 files touched (+256/-75 lines), main `chore/round-11-detekt-baseline`
    merged via `--no-ff`.

**Tech Lead persona**

Tech Lead persona: **PASS**. Disclaimers up front to the user before
starting:
  (a) style fixes have no behavioural change, so no new tests are
      warranted — the existing 198 are the guarantee;
  (b) suppressions are valid resolution when they document an
      informed design decision, and accepted on rules where the
      design IS intentional (LongParameterList on Spring `@Bean`,
      TooManyFunctions on row-source adapters, etc.).
  (c) the one behavioural refactor (`handleEvent` decomposition) was
      kept narrow — same dispatch, same tests, no behaviour change.

## Round 10 — Round-9 follow-ups + Wave 5.2 + parallel V1/orphan/lag drilldown

Six feature/doc branches merged into `main` plus one wiring follow-up
commit, with a small README drift patch between the first two. Round
9 landed Wave 5.1 + the two E2E ITs + the arquitetura docs; Round 10
closes the residual MINORs that the Round 9 architect review flagged,
ships Wave 5.2 (roadmap row 10), and clears the three deferrals row
12 was tracking — all via four parallel agents in worktrees, merged
sequentially into `main` once each landed.

**Branch 1 — `chore/round-9-followups` (cleanup) → merge `9c9a007`**

  * `MysqlRabbitMqE2EIT` now uses Awaitility polling on
    `processor.snapshotState().msSinceLastActivity != Long.MAX_VALUE`
    instead of a hard-coded 1500 ms `Thread.sleep`. The IT is now
    timing-independent and runs ~200 ms faster in the happy path.
  * The verbose `payload.rename: col0 → id` mapping fixture used by
    `MysqlRabbitMqE2EIT` is gone — Wave 5.1 already landed the
    `INFORMATION_SCHEMA` column-name resolution so the test no
    longer needs to apologise for `col0`/`col1`/…
  * Property-name casing standardised across docs:
    `cdc.outbox.health.max-idle` (kebab) in YAML / prose, `maxIdle`
    (camel) only as an Actuator detail-field name where it actually
    appears in `CdcOutboxHealthIndicator` / `CdcProcessorHealthIndicator`.
    Patched `README.md` line ~440 and `docs/ARCHITECTURE.md` line
    ~578; verified those edits ship unchanged in this round.

**Drift patch — `a9c6836`**

After the cleanup merged but before the Wave 5.2 branch was
prepared, the orchestrator noticed the README body prose still
described Wave 5.1 items as "open" in a couple of places (the
Round 9 architect had written from a pre-merge baseline). Commit
`a9c6836 docs(round-9): patch stale Wave 5.1 references in README
body` patched those without touching the roadmap table itself —
that was left for this round.

**Branch 2 — `feat/wave-5.2` (Wave 5.2) → merge `a159fa7`** (commit
`a9c6836` parent, `a9a2e00` feature commit)

  * **New port** `core/port/CheckpointStore` (`load(key) / save(key,
    value)`) for persisting opaque per-source checkpoint markers
    across restarts. Contractual invariants: `save` MUST be atomic
    (crash mid-save cannot leave a corrupted value); `load` tolerates
    corruption with WARN + `null` so the source falls back to its
    natural start position. Single-threaded by orchestrator contract.
  * **New adapter** `adapter/checkpoint/FileCheckpointStore`: one
    JSON file per key under a configurable directory; `save` writes
    a sibling `.tmp`, `FileChannel.force(true)`s (`fsync`), then
    `Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)`. Falls back to
    `REPLACE_EXISTING` (non-atomic) with WARN on filesystems that
    refuse atomic move (some network mounts).
  * **`MySqlBinlogRowChangeSource` wired** to the store: optional
    `CheckpointStore?` constructor arg. `open()` loads
    `"binlog:<serverId>"` and — when valid — programs the binlog
    client via `setBinlogFilename` / `setBinlogPosition`. `ack()`
    persists `<file>:<nextPosition>`. The legacy in-memory path is
    preserved when no store is wired.
  * **Column-name cache invalidation** in the same adapter: when a
    `TABLE_MAP` arrives for a known `tableId` but with a different
    `columnCount`, the cached name list is dropped and re-resolved
    from `INFORMATION_SCHEMA` on the next row event. Closes Tech
    Lead Round-9 MINOR #3.
  * **New adapter** `adapter/source/postgres/PgWalRowChangeSource`:
    Postgres row-level `RowChangeSource` that consumes wal2json `I`,
    `U`, `D` records via the (now extended) `ByteToClassParserImplV2`
    + `SlotMessageV2` / `Wal2JsonColumn`. Emits `RowChange` with
    `before` (`identity` columns) / `after` (`columns` columns)
    column maps. Coexists with `PgLogicalReplicationCdcSource` —
    auto-config picks one or the other by bean wiring; they MUST
    NOT run on the same slot.
  * **Hex pending-failure surface**: `CdcProcessor.ProcessorState`
    gained `pendingFailureCheckpoint: String?`. `CdcProcessorHealthIndicator`
    now reports `DOWN` when it is non-null (precedence: pending >
    not-running > not-iterating > idle > UP). Closes Wave 5.1's
    deferred functional parity with the legacy indicator's
    `pendingFailureLsn`.
  * **Auto-config wiring**: `CdcOutboxHexagonalAutoConfiguration`
    resolves `cdcOutboxSource` as
    `MappingCdcSource(RowChangeSource, MappingRules)` when a
    `RowChangeSource` bean exists (binlog OR new `PgWalRowChangeSource`),
    falling back to `PgLogicalReplicationCdcSource` otherwise. New
    properties: `cdc.outbox.checkpoint.enabled` (default `false`)
    and `cdc.outbox.checkpoint.directory` (default
    `.cdc-outbox-checkpoints`).
  * **Tests**: `FileCheckpointStoreTest`, `InMemoryCheckpointStore`
    test double, `PgWalRowChangeSourceTest`, plus extensions to
    `MySqlBinlogRowChangeSourceTest`, `CdcProcessorTest`, and
    `CdcProcessorHealthIndicatorTest` to cover the new branches.

**Documentation in this round (the work this entry records)**

  * `README.md` — flipped roadmap row 10 from "open" to "Wave 5.2 —
    done", rewrote the cell to enumerate what shipped, added row 12
    for explicit follow-ups (slot/binlog lag-as-gauge,
    `FileCheckpointStore` orphan-`.tmp` sweep). Updated Players
    integrados → Origens to mark `PgWalRowChangeSource` as Pronto
    and to mention `CheckpointStore` wiring on the binlog row.
    Replaced the "paridade entra na Onda 5.2" paragraph in
    Observabilidade with a DONE description using the actual field
    name `pendingFailureCheckpoint`. Added
    `cdc.outbox.checkpoint.{enabled,directory}` to the Quick start
    YAML and to the configuration-surface bullet list. Updated the
    hexagonal Mermaid `flowchart LR` with a `PgWalRowChangeSource`
    node and a dashed `load/save` arrow into a new `Checkpoint`
    subgraph.
  * `docs/ARCHITECTURE.md` — added a `CheckpointStore` subsection
    under "Portas", a `PgWalRowChangeSource` subsection under
    "Adaptadores de origem", a new "Adaptador de checkpoint
    file-backed" top-level subsection covering
    `FileCheckpointStore`'s atomic-save behaviour, and the new
    `cdc.outbox.checkpoint.*` property block. Updated the hex
    health indicator description to drop the "pending entra na
    Onda 5.2" claim and reflect the now-done parity. Added a
    `CheckpointStore` participant to the MySQL binlog → Kafka
    `sequenceDiagram` (one `save("binlog:serverId", "file:pos")`
    arrow after ack, with a caption noting the participant is
    elided when no store is wired). Última atualização rev'd to
    `a159fa7`.
  * `docs/HISTORY.md` — this entry.

**Branch 3 — `docs/round-10` (Architect refresh) → merge `811fdbb`**

Architect agent ran in worktree `/private/tmp/cdc-outbox-docs-r10`
against post-merge `main` at `a159fa7`. Three docs touched: README
(+85/−22), ARCHITECTURE (+152/−42), HISTORY (+133/0) — see the
"Documentation in this round" block above for the per-file detail.
Tech Lead self-walk: PASS with the trims already applied before
commit (V1 row removed from row 12 after confirming the V1 parser
already had polymorphic dispatch; Mermaid quote-and-colon fix on
the `Ckp.save(...)` arrow; ARCHITECTURE.md final length 767 lines,
17 over the soft 750 cap — pragmatic overshoot accepted given the
new port + two new adapter sections).

**Branch 4 — `feat/wave-5.2-orphan-sweep` → merge `f92391b`**

  * `FileCheckpointStore` sweeps orphan `<key>.json.tmp` files at
    construction time, recovering disk left half-committed by a
    crash between `Files.write` and `ATOMIC_MOVE`. Each entry is
    `Files.delete(...)`'d and recorded as
    `cdc.outbox.checkpoint.orphans_swept{outcome=deleted|failed}`.
    Eager-on-construction (rather than an `init()` hop) avoids a
    second-file touch on auto-config, which is in the worker's
    file-allowlist for this commit; rationale documented in the
    class KDoc.
  * `CdcOutboxMetrics` adds `recordCheckpointOrphanSwept(outcome)`
    plus the `CHECKPOINT_ORPHANS_SWEPT` / `TAG_OUTCOME` constants.
  * 7 new `FileCheckpointStoreTest` cases (single + multiple sweep,
    non-`.tmp` left alone, empty/missing dir no-op, idempotent on
    double construction, POSIX-only IO-error path guarded by
    `supportsPosix(...)`).
  * Scope cut: the `CdcOutboxMetrics` ctor arg defaulted to
    `noop()` because auto-config was on the worker's "must not
    touch" list. The orchestrator's follow-up commit `6fd6bab`
    (see below) threads the bean's metrics through.

**Branch 5 — `feat/wave-5.2-v1-columns` → merge `a3ca1d4`**

  * `ByteToClassParserImplV1` translates V1 wal2json rows into the
    canonical `InsertChange` / `UpdateChange` / `DeleteChange`
    subtypes with `columns` / `identity` populated as
    `List<Wal2JsonColumn>`, zipping the parallel `columnnames` /
    `columntypes` / `columnvalues` arrays (and
    `oldkeys.keynames` / `keytypes` / `keyvalues` on `U` / `D`).
    Closes V1↔V2 parity so `PgWalRowChangeSource` works against
    a `format-version=1` slot transparently — no source-side
    edits needed (it dispatches on the in-memory `Change`
    subtype, not on the wire format).
  * **Caveat caught mid-pass:** first draft dropped the
    `pg_logical_emit_message` path on V1, which would have
    `ClassCastException`'d the existing
    `SlotReaderMessageProducerIT format_v1 - without type` case
    at the `change as MessageChange` cast. Fixed by routing
    `kind=message` through the translator and adding a regression
    test.
  * New file `replication/model/v1/V1RowFields.kt` (permissive
    Jackson DTOs `V1RowRecord` + `V1OldKeys`). `SlotMessageV1`
    moved to `List<V1RowRecord>` + nullable wrapper `nextlsn`
    that propagates to every child `Change`.
  * 11 new `ByteToClassParserImplV1ColumnsTest` cases covering
    I / U / D column surfacing, message-record handling, null
    column values, length-mismatched parallel arrays (degrade
    gracefully), wrapper-LSN propagation, truncate-style records,
    malformed input.

**Branch 6 — `feat/wave-5.2-lag-probe` → merge `6d963e9`**

  * **New port** `core/port/LagProbe` returning lag in bytes (or
    `null` when temporarily unavailable). `sourceLabel` field
    carries the tag value used by the Micrometer gauge.
  * **`PostgresLagProbe`** queries
    `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`
    against `pg_replication_slots` for the configured
    `slot_name`. Discriminates SQL `NULL` `confirmed_flush_lsn`
    (slot never streamed from) from a real zero-byte lag using
    `wasNull()`. `SQLException` → WARN log + `null`.
  * **`MysqlLagProbe`** queries `SHOW MASTER STATUS`, parses the
    server's current `(File, Position)`, then reads the
    persisted `binlog:<serverId>` value from the
    `CheckpointStore`. When the files match → returns the byte
    delta; when they diverge (binlog rotated past the checkpoint)
    → returns `null` and logs INFO once (debounced via
    `AtomicBoolean`) per the conservative branch in the brief.
    Negative deltas treated as anomalies and returned as `null`.
  * **`LagProbeScheduler`** wraps a daemon-thread
    `ScheduledExecutorService` that samples the probe at
    `cdc.outbox.lag.interval` (default 10s) and parks the result
    in an `AtomicLong` cache, with `Long.MIN_VALUE` as the
    "no sample yet" sentinel — exposed to Prometheus as
    `Double.NaN`. Decouples Micrometer scrape rate from the
    SQL cost of probing.
  * **`CdcOutboxMetrics.registerLagGauge(sourceLabel, supplier)`**
    registers a `Gauge` reading from the scheduler's cache. New
    constants `SOURCE_LAG_BYTES` (metric name) and `TAG_SOURCE`
    (tag key).
  * **Metric**: `cdc.outbox.source.lag_bytes{source=postgres|mysql}`.
  * **Auto-config**: three new conditional beans in
    `CdcOutboxHexagonalAutoConfiguration` keyed off
    `cdc.outbox.lag.enabled=true` (default) AND off the concrete
    `PgWalRowChangeSource` / `MySqlBinlogRowChangeSource` beans
    via `@ConditionalOnBean`. Consumers can override `LagProbe`
    directly. `CdcOutboxProperties` gains a `Lag` block with
    `enabled` / `interval`.
  * `MySqlBinlogRowChangeSource` exposes `serverId` as a public
    `val` so the lag probe can build the matching
    `binlog:<serverId>` checkpoint key without a second
    properties knob.
  * Tests: `PostgresLagProbeTest`, `MysqlLagProbeTest`,
    `LagProbeSchedulerTest`, `LagProbeAutoConfigurationTest`,
    plus 2 new `CdcOutboxMetricsTest` cases.
  * Merge conflict resolution: `CdcOutboxMetrics(.kt|Test.kt)`
    were touched by branches 4 and 6 in different regions
    (counter vs gauge surface). Both sides kept verbatim — the
    APIs are additive and independent.

**Follow-up commit — `6fd6bab`**

`fix(metrics): thread CdcOutboxMetrics into FileCheckpointStore`
closes the scope cut declared by Branch 4. The bean factory in
`CdcOutboxHexagonalAutoConfiguration` now injects the
application's `CdcOutboxMetrics` into the `FileCheckpointStore`
ctor; without this the
`cdc.outbox.checkpoint.orphans_swept{outcome}` counter would
have stayed on the no-op facade in production deployments and
operators wouldn't see crash-recovery signal.

**Documentation gap acknowledged**

The Architect's Branch 3 docs were written BEFORE branches 4–6
landed (the four agents ran in parallel against a common base).
README row 12 still lists the lag-as-gauge and orphan-`.tmp`
items as deferred, and neither the README nor ARCHITECTURE
describes the new `LagProbe` port, V1 column surfacing, or the
orphan-sweep adapter behaviour. The orchestrator's next commit
patches the three files to flip row 12 to "done", add a
`LagProbe` subsection to ARCHITECTURE, and note V1↔V2 parser
parity in the Origens table.

**Verification (post-merge)**

  * `./gradlew compileKotlin compileTestKotlin` PASS on JDK 21
    (Corretto host; project source/target 17).
  * Unit-test sweep (default, no `RUN_TESTCONTAINERS`):
    198 tests, 195 successes, 0 failures, 3 skipped.
  * **Full sweep with `RUN_TESTCONTAINERS=1` +
    `DOCKER_API_VERSION=1.43` (OrbStack)**:
    **198 tests, 198 successes, 0 failures, 0 skipped**. The
    three E2E ITs (`PostgresSnsE2EIT`, `MysqlRabbitMqE2EIT`,
    `AtLeastOnceDeliveryIT`) actually exercise their full chains
    against real containers.

**Late fix uncovered by the Testcontainers run**

`MysqlRabbitMqE2EIT` was constructing `MySqlBinlogRowChangeSource`
without a `dataSource`. Round 9 cleanup had removed the
`payload.rename: col0 → id` workaround mapping on the assumption
that Wave 5.1's INFORMATION_SCHEMA column-name resolution covered
it — but that resolution path requires a `DataSource` to be
wired in, and `mysql-binlog-connector-java 0.29.2` does NOT
surface column names from the binlog metadata even with
`binlog_row_metadata=FULL`. Without the DataSource the source
fell back to `col0`/`col1`/… and the mapping `include`
projection emitted `{}` payloads. Caught only because Round 10
actually ran the gated IT. Fixed by wiring a small HikariDataSource
in the IT's `@BeforeAll`; production wiring through
`CdcOutboxAutoConfiguration` was always correct.

**Tech Lead persona**

Tech Lead persona: **PASS**. All four agent-delivered branches
landed cleanly, conflicts resolved additively, full sweep green
under both default and Testcontainers modes. One regression
caught and fixed inline (MysqlRabbitMqE2EIT DataSource wiring).
Detekt baseline cleanup, Wave 6 multi-module split, and optional
`/ultrareview` defer to next cycles per the round 10 closeout
table.

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
