// AWS sinks — SnsEventSink and SqsEventSink. Spring Cloud AWS 3.x
// templates are `compileOnly` so consumers without Boot still
// compile; the AWS SDK v2 clients are required at runtime.
dependencies {
    api(project(":core"))
    compileOnly("io.awspring.cloud:spring-cloud-aws-sns:4.1.0")
    compileOnly("io.awspring.cloud:spring-cloud-aws-sqs:4.1.0")
    implementation("software.amazon.awssdk:sns:2.44.7")
    implementation("software.amazon.awssdk:sqs:2.44.7")
    implementation("software.amazon.awssdk:sts:2.44.7")

    testImplementation("io.awspring.cloud:spring-cloud-aws-sns:4.1.0")
    testImplementation("io.awspring.cloud:spring-cloud-aws-sqs:4.1.0")
}
