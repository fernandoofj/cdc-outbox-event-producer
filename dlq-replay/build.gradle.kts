// DLQ Replay tooling — operator-facing endpoints to peek, replay, and
// abandon dead-lettered messages. Opt-in via
// `cdc.outbox.dlq.replay.enabled=true`; auto-config refuses to wire
// the endpoint when Spring Security is not on the classpath, and
// each operation enforces authentication at runtime regardless of
// the consumer's filter-chain configuration.
//
// Reads the same envelope shape that `SqsDeadLetterSink` writes
// (flat JSON map with `originalPrefix` / `lsn` / `content` /
// `failureType` / `failureMessage` / `deadLetteredAt`) — no
// envelope migration; DLQs already in production are replayable
// directly.
//
// Replay re-injects into the existing `EventSinkRegistry`, so the
// publish path is byte-identical to a fresh event going through
// the producer.
dependencies {
    api(project(":core"))

    implementation("software.amazon.awssdk:sqs:2.27.21")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.21.3")

    compileOnly("org.springframework.boot:spring-boot-actuator:3.3.5")
    compileOnly("org.springframework.boot:spring-boot-autoconfigure:3.3.5")
    compileOnly("org.springframework.security:spring-security-core:6.3.4")

    testImplementation(project(":test-support"))
    testImplementation("org.springframework.boot:spring-boot-actuator:3.3.5")
    testImplementation("org.springframework.boot:spring-boot-test:3.3.5")
    testImplementation("org.springframework.boot:spring-boot-test-autoconfigure:3.3.5")
    testImplementation("org.springframework.security:spring-security-core:6.3.4")
    testImplementation("org.springframework.security:spring-security-test:6.3.4")
    testImplementation("org.testcontainers:localstack:1.20.4")
    testImplementation("org.assertj:assertj-core:3.25.3")
}
