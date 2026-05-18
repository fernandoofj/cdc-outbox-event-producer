// Legacy message-producer chain — pre-Wave-5 SlotReaderMessageProducer
// + DestinationType/SNSProducer/SQSProducer + the legacy DeadLetterSink
// surface and the LegacyDeadLetterPortAdapter that bridges it to the
// hexagonal DeadLetterPort. Kept as a separate module so consumers
// can opt out by simply not depending on it.
dependencies {
    api(project(":core"))
    api(project(":source-postgres"))
    api(project(":sink-aws"))
    compileOnly("io.awspring.cloud:spring-cloud-aws-sns:4.0.2")
    compileOnly("io.awspring.cloud:spring-cloud-aws-sqs:4.0.2")

    testImplementation("io.awspring.cloud:spring-cloud-aws-sns:4.0.2")
    testImplementation("io.awspring.cloud:spring-cloud-aws-sqs:4.0.2")
    testImplementation(project(":test-support"))
}
