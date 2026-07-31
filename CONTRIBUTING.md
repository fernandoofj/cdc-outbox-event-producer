# Contributing

Thanks for considering a contribution to `cdc-outbox-event-producer`.
This is a personal open-source project maintained on a best-effort
basis; the notes below describe how the codebase is organized and
what a pull request needs to be mergeable.

## Getting started

Requires **JDK 21** as `JAVA_HOME` — it's both the compile target
(Spring Boot 4's own minimum baseline, Round 22) and needed for Gradle
8.10.2 itself: its embedded Kotlin DSL compiler can't parse a two-digit
`java -version` (fails with a bare `26.0.1`-style error on JDK 24+).

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
5. GitHub Actions is currently disabled on this repo, so there is no
   automated CI check on PRs — `./gradlew build` locally (step 3) is
   the gate. If your change touches a source/sink adapter, also run
   the Testcontainers-backed `*IT.kt` suites locally (see
   [README](README.md#testes-e-build-local)); they aren't covered by
   step 3 alone.

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
  * `./gradlew detekt` and `./gradlew ktlintCheck` must both be clean
    (Round 23 closed out the ktlint backlog — the whole tree passes
    `./gradlew build` with zero exclusions now). Any `@Suppress` needs
    a one-line justification comment immediately above it.

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

## Maven Central publish (NF11 — not started)

Today every module publishes to GitHub Packages only (see
`build.gradle.kts` § `publishing`). Maven Central needs three things
this repo's own build config cannot provide — they require account
setup and identity proof only the maintainer can do:

  1. **A Sonatype Central account** (<https://central.sonatype.com>)
     with a registered namespace. The group ID here is
     `br.com.fltech.outbox` — Central requires proving ownership of
     `fltech.com` via a DNS TXT record (`_sonatype-central` or similar,
     the exact record name is issued per-namespace at registration
     time), not just a GitHub account. If `fltech.com` isn't a domain
     you control, either provision a domain-based namespace you do
     control, or move the coordinate to `io.github.<username>`
     (verified by proving control of the corresponding GitHub account
     instead of DNS — the same class of check Round 22 rejected for
     `io.github.cdc` because it wasn't this project's account; a
     correctly-scoped `io.github.fernandoofj` would pass).
  2. **A GPG key pair.** Central requires every published artifact
     signed. Generate one (`gpg --full-generate-key`), publish the
     public key to a keyserver (`gpg --keyserver keyserver.ubuntu.com
     --send-keys <key-id>`), and keep the private key + passphrase out
     of the repo — Gradle's `signing` plugin reads them from
     `~/.gradle/gradle.properties` or environment variables
     (`ORG_GRADLE_PROJECT_signingKey`,
     `ORG_GRADLE_PROJECT_signingPassword`), never committed.
  3. **Gradle wiring**: apply the `signing` plugin plus either
     Sonatype's `central-publishing-gradle-plugin` or the community
     `com.gradleup.nmcp` plugin, pointed at the verified namespace from
     step 1 and signing with the key from step 2. Not added to
     `build.gradle.kts` yet — wiring it against an unverified/unowned
     namespace would produce build config nobody can actually run,
     which is worse than no config.

Once 1–2 are done, wiring 3 is a small, mechanical addition mirroring
the existing GitHub Packages `publishing` block.

## Reporting bugs / requesting features

Open a GitHub issue. Include the source (Postgres/MySQL) and sink
(SNS/SQS/Kafka/RabbitMQ) combination you're using, the relevant
`cdc.outbox.*` configuration, and, for bugs, logs around the failure.

## Security issues

Please do not open a public issue for a security vulnerability — see
[SECURITY.md](SECURITY.md) instead.
