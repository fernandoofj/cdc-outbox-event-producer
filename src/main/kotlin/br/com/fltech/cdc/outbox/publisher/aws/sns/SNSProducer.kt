package br.com.fltech.cdc.outbox.publisher.aws.sns

import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessage

interface SNSProducer {

    fun <T : Any> send(topicName: String, message: SNSMessage<T>)
}
