package shop.inventa.pg2sns4k.workflow

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import io.awspring.cloud.messaging.core.NotificationMessagingTemplate
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import shop.inventa.pg2sns4k.aws.sns.SNSTransactionalProducer
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessageMother
import shop.inventa.pg2sns4k.common.IntegrationBase
import shop.inventa.pg2sns4k.jackson.MappingJackson2MessageConverterCustom
import kotlin.test.assertEquals

internal class SlotReaderSNSProducerIT : IntegrationBase() {

    private lateinit var slotReaderSNSProducer: SlotReaderSNSProducer

    @BeforeAll
    override fun setUp() {
        super.setUpBegin()

        val isTestingExecution = true
        val snsTransactionalProducer = SNSTransactionalProducer(
            notificationMessagingTemplate = NotificationMessagingTemplate(
                AmazonSNSClientBuilder
                    .standard()
                    .withEndpointConfiguration(
                        AwsClientBuilder.EndpointConfiguration(
                            this.awsParamaters.localstackUrl,
                            this.awsParamaters.region
                        )
                    )
                    .withCredentials(
                        AWSStaticCredentialsProvider(
                            BasicAWSCredentials(
                                this.awsParamaters.awsAccessKey,
                                this.awsParamaters.awsSecretKey
                            )
                        )
                    )
                    .build()
            ).apply {
                messageConverter = MappingJackson2MessageConverterCustom(ObjectMapper()).jackson2MessageConverter()
            }
        )

        slotReaderSNSProducer = SlotReaderSNSProducer(
            postgresConfiguration,
            replicationConfiguration,
            snsTransactionalProducer,
            isTestingExecution
        )
    }

    @AfterAll
    override fun tearDown() {
        super.tearDownEnd()
    }

    @Test
    fun `read one message from slot in testing mode`() {
        // given
        val snsMessageString = objectMapper.writeValueAsString(SNSMessageMother.build())
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, 'test-business-events', '$snsMessageString')"

        // when / then
        val emitMessageResult = executeCommand(emitMessageCommand)
        assertEquals(true, emitMessageResult)

        slotReaderSNSProducer.startStreaming()
    }
}
