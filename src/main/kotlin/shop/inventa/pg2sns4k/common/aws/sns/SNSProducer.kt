package shop.inventa.pg2sns4k.common.aws.sns

import shop.inventa.pg2sns4k.common.aws.sns.dto.SNSMessage

interface SNSProducer {

    fun <T : Any> send(topicName: String, message: SNSMessage<T>)
}
