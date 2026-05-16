rootProject.name = "cdc-outbox-event-producer"

// Wave 6 — multi-module split. Each adapter family is its own
// Gradle module so the hexagonal boundary is enforced at the build
// graph rather than relying on package discipline alone. `core` has
// zero framework deps; adapters depend on `core` plus their own
// driver libs (compileOnly where the consumer brings their own);
// `spring-boot-starter` is the only module that knows the full
// surface and wires the auto-configs.
include(
    ":core",
    ":checkpoint-file",
    ":source-postgres",
    ":source-mysql",
    ":source-stubs",
    ":sink-composition",
    ":sink-aws",
    ":sink-kafka",
    ":sink-rabbitmq",
    ":lag-probes",
    ":legacy",
    ":dlq-replay",
    ":replay-source",
    ":spring-boot-starter",
    ":test-support",
)
