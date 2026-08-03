// Kafka sink — depends on spring-kafka KafkaTemplate. compileOnly
// so consumers that only use SNS/SQS don't pay for Kafka transitive
// deps; the auto-config wires this sink only when KafkaTemplate is
// on the classpath.
dependencies {
    api(project(":core"))
    compileOnly("org.springframework.kafka:spring-kafka:4.1.0")

    testImplementation("org.springframework.kafka:spring-kafka:4.1.0")
}
