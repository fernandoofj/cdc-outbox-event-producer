// Shared test fixtures (E2EContainers, InMemoryCheckpointStore test
// double, etc.). Exposed under the `main` source set so consumer
// modules can `testImplementation(project(":test-support"))` without
// jumping through testFixtures wiring. Production code does NOT
// depend on this module.
dependencies {
    api(project(":core"))
    api(project(":source-postgres"))
    api(project(":source-mysql"))
    api(project(":sink-aws"))
    api(project(":sink-rabbitmq"))
    api(project(":checkpoint-file"))

    api("org.junit.jupiter:junit-jupiter:5.10.3")
    api("org.testcontainers:testcontainers:1.21.4")
    api("org.testcontainers:junit-jupiter:1.21.4")
    api("org.testcontainers:postgresql:1.21.4")
    api("org.testcontainers:localstack:1.21.4")
    api("org.testcontainers:mysql:1.21.4")
    api("org.testcontainers:rabbitmq:1.21.4")
    api("org.awaitility:awaitility:4.2.2")

    api("com.mysql:mysql-connector-j:8.4.0")
    api("com.zendesk:mysql-binlog-connector-java:0.29.2")

    api("io.awspring.cloud:spring-cloud-aws-sns:3.2.1")
    api("io.awspring.cloud:spring-cloud-aws-sqs:3.2.1")
    api("org.springframework.amqp:spring-rabbit:3.1.7")
    api("org.springframework.boot:spring-boot-autoconfigure:4.0.6")
    api("org.springframework.boot:spring-boot-test:4.0.6")
}
