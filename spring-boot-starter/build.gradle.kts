// Spring Boot starter — the only module that knows the full surface.
// Wires the auto-configs that pick up adapters present on the
// classpath via `@ConditionalOnBean` / `@ConditionalOnClass`. All
// adapter modules are `compileOnly` so consumers can drop the ones
// they don't use without classpath bloat.
plugins {
    id("org.jetbrains.kotlin.kapt")
}

dependencies {
    api(project(":core"))

    compileOnly(project(":checkpoint-file"))
    compileOnly(project(":source-postgres"))
    compileOnly(project(":source-mysql"))
    compileOnly(project(":source-stubs"))
    compileOnly(project(":sink-composition"))
    compileOnly(project(":sink-aws"))
    compileOnly(project(":sink-kafka"))
    compileOnly(project(":sink-rabbitmq"))
    compileOnly(project(":lag-probes"))
    compileOnly(project(":legacy"))
    compileOnly(project(":dlq-replay"))
    compileOnly(project(":replay-source"))

    compileOnly("org.springframework.boot:spring-boot-autoconfigure:3.3.5")
    compileOnly("org.springframework.boot:spring-boot-actuator:3.3.5")
    compileOnly("org.springframework.boot:spring-boot-actuator-autoconfigure:3.3.5")
    kapt("org.springframework.boot:spring-boot-configuration-processor:3.3.5")

    compileOnly("io.awspring.cloud:spring-cloud-aws-sns:3.2.1")
    compileOnly("io.awspring.cloud:spring-cloud-aws-sqs:3.2.1")
    compileOnly("org.springframework.kafka:spring-kafka:3.2.4")
    compileOnly("org.springframework.amqp:spring-rabbit:3.1.7")

    compileOnly("com.mysql:mysql-connector-j:8.4.0")
    compileOnly("com.zendesk:mysql-binlog-connector-java:0.29.2")
    compileOnly("org.springframework.security:spring-security-web:6.3.4")

    testImplementation(project(":checkpoint-file"))
    testImplementation(project(":source-postgres"))
    testImplementation(project(":source-mysql"))
    testImplementation(project(":sink-aws"))
    testImplementation(project(":sink-kafka"))
    testImplementation(project(":sink-rabbitmq"))
    testImplementation(project(":sink-composition"))
    testImplementation(project(":lag-probes"))
    testImplementation(project(":legacy"))
    testImplementation(project(":dlq-replay"))
    testImplementation(project(":replay-source"))
    testImplementation(project(":test-support"))

    testImplementation("org.springframework.boot:spring-boot-autoconfigure:3.3.5")
    testImplementation("org.springframework.boot:spring-boot-actuator:3.3.5")
    testImplementation("org.springframework.boot:spring-boot-actuator-autoconfigure:3.3.5")
    testImplementation("org.springframework.boot:spring-boot-test:3.3.5")
    testImplementation("org.springframework.boot:spring-boot-test-autoconfigure:3.3.5")
    testImplementation("org.assertj:assertj-core:3.25.3")
    testImplementation("com.zaxxer:HikariCP:5.1.0")
    testImplementation("io.micrometer:micrometer-test:1.16.5")
    testImplementation("com.mysql:mysql-connector-j:8.4.0")
    testImplementation("com.zendesk:mysql-binlog-connector-java:0.29.2")
}
