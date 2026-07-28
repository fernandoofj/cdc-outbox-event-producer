# Contributing

Thanks for considering a contribution to `cdc-outbox-event-producer`.
This is a personal open-source project maintained on a best-effort
basis; the notes below describe how the codebase is organized and
what a pull request needs to be mergeable.

## Getting started

```sh
git clone https://github.com/fernandoofj/cdc-outbox-event-producer.git
cd cdc-outbox-event-producer

# unit tests (no Docker required)
./gradlew test

# start Postgres + LocalStack for the Testcontainers-backed suites
./gradlew startDockerCompose
RUN_TESTCONTAINERS=1 ./gradlew test
./gradlew stopDockerCompose
```

See the [README](README.md#testes-e-build-local) for the full local
toolchain (Kotlin, JVM target, Gradle wrapper versions) and
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the module layout.

## Workflow

1. Fork the repo and branch from `main`.
2. Keep commits small and focused — one logical change per commit.
3. Run `./gradlew build` locally (compiles every module, runs unit
   tests, and enforces zero Detekt findings) before opening a PR.
4. Open a pull request against `main` describing what changed and
   why. Link any related issue.
5. CI (`.github/workflows/ci.yml`) runs the same build + Detekt gate
   on every PR. Testcontainers-backed integration tests (`*IT.kt`)
   are not run in CI — see the workflow file for why — so please run
   them locally if your change touches a source/sink adapter.

## Code style

  * Idiomatic Kotlin: data classes for value objects, sealed classes
    for closed hierarchies, no `lateinit var` where a constructor
    parameter works.
  * Hexagonal architecture is enforced at the build graph: domain
    types live in `core/` with zero framework imports; adapters
    depend on `core`, never the reverse; Spring wiring lives only in
    `spring-boot-starter/`. New code that violates this boundary
    won't compile.
  * Public classes and methods get a short KDoc explaining *why*, not
    *what* — comments that restate the code are not useful.
  * No silent failure paths: a defensive branch that skips an
    operation must log at WARN/ERROR and increment a Micrometer
    counter via `CdcOutboxMetrics`.
  * `./gradlew detekt` must be clean. Any `@Suppress` needs a one-line
    justification comment immediately above it.

## Delivery guarantee

This project promises **at-least-once** delivery. Any change touching
the replication loop, checkpointing, or the LSN/offset advance logic
must preserve these invariants (see [README](README.md) and
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for details):

  1. A checkpoint only advances past a message once that message has
     been published, explicitly discarded by a documented rule, or
     dead-lettered.
  2. On publisher failure, the loop retries the same message with
     exponential backoff + jitter before dead-lettering it.
  3. Graceful shutdown finishes the in-flight message and advances
     the checkpoint before closing connections — no partial state.

PRs that weaken these guarantees, even for a niche broker or
database, will be asked to change.

## Reporting bugs / requesting features

Open a GitHub issue. Include the source (Postgres/MySQL) and sink
(SNS/SQS/Kafka/RabbitMQ) combination you're using, the relevant
`cdc.outbox.*` configuration, and, for bugs, logs around the failure.

## Security issues

Please do not open a public issue for a security vulnerability — see
[SECURITY.md](SECURITY.md) instead.
