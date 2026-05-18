// RabbitMQ sink — depends on spring-amqp RabbitTemplate. Same
// `compileOnly` pattern as sink-kafka.
dependencies {
    api(project(":core"))
    compileOnly("org.springframework.amqp:spring-rabbit:4.0.3")

    testImplementation("org.springframework.amqp:spring-rabbit:4.0.3")
}
