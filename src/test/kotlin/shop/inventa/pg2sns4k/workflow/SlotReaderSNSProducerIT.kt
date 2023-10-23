package shop.inventa.pg2sns4k.workflow

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.awspring.cloud.messaging.core.NotificationMessagingTemplate
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import shop.inventa.pg2sns4k.aws.sns.SNSTransactionalProducer
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessageMother
import shop.inventa.pg2sns4k.common.IntegrationBase
import shop.inventa.pg2sns4k.jackson.MappingJackson2MessageConverterCustom
import java.io.FileInputStream
import java.util.Properties
import kotlin.test.assertEquals

internal class SlotReaderSNSProducerIT : IntegrationBase() {

    private lateinit var slotReaderSNSProducer: SlotReaderSNSProducer

    @BeforeAll
    override fun setUp() {

        super.setUpBegin()

        val properties = loadProperties()
        val isTestingExecution = true
        val awsAccessKey = properties["cloud.aws.credentials.access-key"].toString()
        val secretKey = properties["cloud.aws.credentials.secret-key"].toString()
        val region = properties["cloud.aws.region.static"].toString()
        val localStackUrl = properties["cloud.aws.localstack.url"].toString()

        val snsTransactionalProducer = SNSTransactionalProducer(
            notificationMessagingTemplate = NotificationMessagingTemplate(
                AmazonSNSClientBuilder
                    .standard()
                    .withEndpointConfiguration(
                        AwsClientBuilder.EndpointConfiguration(localStackUrl, region)
                    )
                    .withCredentials(
                        AWSStaticCredentialsProvider(BasicAWSCredentials(awsAccessKey, secretKey))
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
        val snsMessageString = defaultMapper().writeValueAsString(SNSMessageMother.build())
        val createSlotCommand = "SELECT pg_create_logical_replication_slot('catalog_slot', 'wal2json')"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, 'catalogue-collection-business-events', '$snsMessageString')"

        // when / then
        val createSlotResult = executeCommand(createSlotCommand)
        assertEquals(true, createSlotResult)

        val emitMessageResult = executeCommand(emitMessageCommand)
        assertEquals(true, emitMessageResult)

        slotReaderSNSProducer.startStreaming()
    }

    companion object {
        private fun defaultMapper(): ObjectMapper {
            val objectMapper = ObjectMapper()
            objectMapper.registerModule(JavaTimeModule())
            objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            return objectMapper
        }

        private fun loadProperties(): Properties {
            val properties = Properties()
            val inputStream = FileInputStream("src/test/resources/application.properties")
            properties.load(inputStream)
            return properties
        }
    }
}
