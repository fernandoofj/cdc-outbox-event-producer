package br.com.fltech.outbox.publisher.aws.sqs

import io.awspring.cloud.sqs.operations.SqsTemplate

/**
 * SQS producer adapter backed by Spring Cloud AWS 3.x's [SqsTemplate].
 *
 * Replaces the SCA-2 `QueueMessagingTemplate` (AWS SDK v1). The public
 * surface of this class is unchanged. The class is a plain Kotlin POJO;
 * the Spring bean lifecycle is handled by the auto-configuration so the
 * library remains usable without Spring on the classpath.
 */
class SQSTransactionalProducer(
    private val sqsTemplate: SqsTemplate,
) : SQSProducer {
    override fun <T : Any> send(
        queueName: String,
        message: T,
    ) {
        sqsTemplate.send { it.queue(queueName).payload(message) }
    }
}
