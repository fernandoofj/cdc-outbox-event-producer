package shop.inventa.pg2sns4k.workflow

import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.runs
import io.mockk.verify
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import shop.inventa.pg2sns4k.aws.common.PayloadMother
import shop.inventa.pg2sns4k.aws.sns.SNSProducer
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessageMother
import shop.inventa.pg2sns4k.aws.sqs.SQSProducer
import shop.inventa.pg2sns4k.common.IntegrationBase
import shop.inventa.pg2sns4k.jackson.ObjectMapperSingleton.defaultMapper
import shop.inventa.pg2sns4k.replication.enums.FormatVersionEnum
import java.util.concurrent.Executors
import kotlin.test.assertEquals

@ExtendWith(MockKExtension::class)
internal class SlotReaderMessageProducerIT : IntegrationBase() {

    private lateinit var slotReaderMessageProducer: SlotReaderMessageProducer

    @MockK
    private lateinit var snsProducer: SNSProducer

    @MockK
    private lateinit var sqsProducer: SQSProducer

    @BeforeAll
    override fun setUp() {
        super.setUpBegin()
    }

    @AfterAll
    override fun tearDown() {
        super.tearDownEnd()
    }

    @Test
    fun `format_v1 - without type - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V1)
        val snsMessage = SNSMessageMother.build()
        val snsMessageString = defaultMapper.writeValueAsString(snsMessage)
        val topicName = "test-business-events"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$topicName', '$snsMessageString')"

        every {
            snsProducer.send(topicName, snsMessage)
        } just runs

        // when
        val emitMessageResult = executeCommand(emitMessageCommand)
        assertEquals(true, emitMessageResult)

        val executorService = Executors.newSingleThreadExecutor()
        executorService.submit {
            Thread.sleep(2000)
            slotReaderMessageProducer.stopStreaming()
        }

        slotReaderMessageProducer.startStreaming()

        // then
        verify(exactly = 1) {
            snsProducer.send(topicName, snsMessage)
        }
    }

    @Test
    fun `format_v2 - without type - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V2)
        val snsMessage = SNSMessageMother.build()
        val snsMessageString = defaultMapper.writeValueAsString(snsMessage)
        val topicName = "test-business-events"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$topicName', '$snsMessageString')"

        every {
            snsProducer.send(topicName, snsMessage)
        } just runs

        // when
        val emitMessageResult = executeCommand(emitMessageCommand)
        assertEquals(true, emitMessageResult)

        val executorService = Executors.newSingleThreadExecutor()
        executorService.submit {
            Thread.sleep(2000)
            slotReaderMessageProducer.stopStreaming()
        }

        slotReaderMessageProducer.startStreaming()

        // then
        verify(exactly = 1) {
            snsProducer.send(topicName, snsMessage)
        }
    }

    @Test
    fun `format_v1 - SNS - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V1)
        val snsMessage = SNSMessageMother.build()
        val snsMessageString = defaultMapper.writeValueAsString(snsMessage)
        val prefix = "SNS|test-business-events"
        val topicName = "test-business-events"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$prefix', '$snsMessageString')"

        every {
            snsProducer.send(topicName, snsMessage)
        } just runs

        // when
        val emitMessageResult = executeCommand(emitMessageCommand)
        assertEquals(true, emitMessageResult)

        val executorService = Executors.newSingleThreadExecutor()
        executorService.submit {
            Thread.sleep(2000)
            slotReaderMessageProducer.stopStreaming()
        }

        slotReaderMessageProducer.startStreaming()

        // then
        verify(exactly = 1) {
            snsProducer.send(topicName, snsMessage)
        }
    }

    @Test
    fun `format_v2 - SNS - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V2)
        val snsMessage = SNSMessageMother.build()
        val snsMessageString = defaultMapper.writeValueAsString(snsMessage)
        val prefix = "SNS|test-business-events"
        val topicName = "test-business-events"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$prefix', '$snsMessageString')"

        every {
            snsProducer.send(topicName, snsMessage)
        } just runs

        // when
        val emitMessageResult = executeCommand(emitMessageCommand)
        assertEquals(true, emitMessageResult)

        val executorService = Executors.newSingleThreadExecutor()
        executorService.submit {
            Thread.sleep(2000)
            slotReaderMessageProducer.stopStreaming()
        }

        slotReaderMessageProducer.startStreaming()

        // then
        verify(exactly = 1) {
            snsProducer.send(topicName, snsMessage)
        }
    }

    @Test
    fun `format_v1 - SQS - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V1)
        val payload = PayloadMother.build()
        val sqsPayloadString = defaultMapper.writeValueAsString(payload)
        val prefix = "SQS|test-business-events"
        val queueName = "test-business-events"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$prefix', '$sqsPayloadString')"

        every {
            sqsProducer.send(queueName, sqsPayloadString)
        } just runs

        // when
        val emitMessageResult = executeCommand(emitMessageCommand)
        assertEquals(true, emitMessageResult)

        val executorService = Executors.newSingleThreadExecutor()
        executorService.submit {
            Thread.sleep(2000)
            slotReaderMessageProducer.stopStreaming()
        }

        slotReaderMessageProducer.startStreaming()

        // then
        verify(exactly = 1) {
            sqsProducer.send(queueName, sqsPayloadString)
        }
    }

    @Test
    fun `format_v2 - SQS - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V2)
        val payload = PayloadMother.build()
        val sqsPayloadString = defaultMapper.writeValueAsString(payload)
        val prefix = "SQS|test-business-events"
        val queueName = "test-business-events"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$prefix', '$sqsPayloadString')"

        every {
            sqsProducer.send(queueName, sqsPayloadString)
        } just runs

        // when
        val emitMessageResult = executeCommand(emitMessageCommand)
        assertEquals(true, emitMessageResult)

        val executorService = Executors.newSingleThreadExecutor()
        executorService.submit {
            Thread.sleep(2000)
            slotReaderMessageProducer.stopStreaming()
        }

        slotReaderMessageProducer.startStreaming()

        // then
        verify(exactly = 1) {
            sqsProducer.send(queueName, sqsPayloadString)
        }
    }

    private fun buildSlotReaderProducer(formatVersion: FormatVersionEnum): SlotReaderMessageProducer {
        replicationConfiguration = replicationConfiguration.copy(formatVersion = formatVersion)

        return SlotReaderMessageProducer(
            postgresConfiguration,
            replicationConfiguration,
            snsProducer,
            sqsProducer
        )
    }
}
