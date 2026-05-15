package br.com.fltech.cdc.outbox.publisher.aws.sqs

interface SQSProducer {
    fun <T : Any> send(queueName: String, message: T)
}
