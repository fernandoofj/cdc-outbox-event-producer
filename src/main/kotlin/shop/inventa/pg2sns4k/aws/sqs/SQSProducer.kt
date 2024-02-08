package shop.inventa.pg2sns4k.aws.sqs

interface SQSProducer {
    fun <T : Any> send(queueName: String, message: T)
}
