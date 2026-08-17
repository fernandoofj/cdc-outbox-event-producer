// Spring Boot starter — the only module that knows the full surface.
// Wires the auto-configs that pick up adapters present on the
// classpath via `@ConditionalOnBean` / `@ConditionalOnClass`. Most
// adapter modules are `compileOnly` so consumers can drop the ones
// they don't use without classpath bloat.
plugins {
    id("org.jetbrains.kotlin.kapt")
}

dependencies {
    api(project(":core"))

    // Not compileOnly like the adapters below: CdcOutboxSinkAutoConfiguration's
    // cdcOutboxSinkRegistry bean unconditionally builds a
    // DefaultEventSinkRegistry, regardless of how many sinks end up
    // wired — the hexagonal (default) chain cannot start without it,
    // so a consumer who only declares `starter` + `source-postgres` +
    // `sink-aws` (README's own "Setup mínimo") must get it for free.
    implementation(project(":sink-composition"))

    compileOnly(project(":checkpoint-file"))
    compileOnly(project(":source-postgres"))
    compileOnly(project(":source-mysql"))
    compileOnly(project(":source-stubs"))
    compileOnly(project(":sink-aws"))
    compileOnly(project(":sink-kafka"))
    compileOnly(project(":sink-rabbitmq"))
    compileOnly(project(":lag-probes"))
    compileOnly(project(":legacy"))
    compileOnly(project(":dlq-replay"))
    compileOnly(project(":replay-source"))

    compileOnly("org.springframework.boot:spring-boot-autoconfigure:4.0.6")
    compileOnly("org.springframework.boot:spring-boot-actuator:4.0.6")
    compileOnly("org.springframework.boot:spring-boot-actuator-autoconfigure:4.0.6")
    // Boot 4 extracted Health from spring-boot-actuator into its own
    // module; HealthIndicator now lives under org.springframework.boot.health.contributor.
    compileOnly("org.springframework.boot:spring-boot-health:4.0.6")
    kapt("org.springframework.boot:spring-boot-configuration-processor:4.0.6")

    compileOnly("io.awspring.cloud:spring-cloud-aws-sns:4.1.0")
    compileOnly("io.awspring.cloud:spring-cloud-aws-sqs:4.1.0")
    compileOnly("org.springframework.kafka:spring-kafka:4.1.0")
    compileOnly("org.springframework.amqp:spring-rabbit:4.1.0")

    // Test-only: the real Spring Cloud AWS autoconfiguration classes,
    // to regression-test that CdcOutboxSinkAutoConfiguration's
    // @AutoConfigureAfter ordering against them is actually correct —
    // a fake test double's @AutoConfiguration wouldn't prove anything
    // about the real SnsAutoConfiguration/SqsAutoConfiguration's own
    // bean registration timing.
    testImplementation("io.awspring.cloud:spring-cloud-aws-autoconfigure:4.1.0")

    compileOnly("com.mysql:mysql-connector-j:8.4.0")
    compileOnly("com.zendesk:mysql-binlog-connector-java:0.29.2")
    compileOnly("org.springframework.security:spring-security-web:7.0.5")

    testImplementation(project(":checkpoint-file"))
    testImplementation(project(":source-postgres"))
    testImplementation(project(":source-mysql"))
    testImplementation(project(":sink-aws"))
    testImplementation(project(":sink-kafka"))
    testImplementation(project(":sink-rabbitmq"))
    testImplementation(project(":lag-probes"))
    testImplementation(project(":legacy"))
    testImplementation(project(":dlq-replay"))
    testImplementation(project(":replay-source"))
    testImplementation(project(":test-support"))

    testImplementation("org.springframework.boot:spring-boot-autoconfigure:4.0.6")
    testImplementation("org.springframework.boot:spring-boot-actuator:4.0.6")
    testImplementation("org.springframework.boot:spring-boot-actuator-autoconfigure:4.0.6")
    testImplementation("org.springframework.boot:spring-boot-health:4.0.6")
    testImplementation("org.springframework.boot:spring-boot-test:4.0.6")
    testImplementation("org.springframework.boot:spring-boot-test-autoconfigure:4.0.6")
    testImplementation("org.assertj:assertj-core:3.25.3")
    testImplementation("com.zaxxer:HikariCP:7.0.2")
    testImplementation("io.micrometer:micrometer-test:1.16.5")
    testImplementation("com.mysql:mysql-connector-j:8.4.0")
    testImplementation("com.zendesk:mysql-binlog-connector-java:0.29.2")
}
