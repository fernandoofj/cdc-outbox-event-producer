package br.com.fltech.outbox.publisher.workflow

import br.com.fltech.outbox.publisher.aws.common.PayloadMother
import br.com.fltech.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessageMother
import br.com.fltech.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.outbox.publisher.common.IntegrationBase
import br.com.fltech.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.outbox.publisher.replication.enums.FormatVersionEnum
import io.mockk.CapturingSlot
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.runs
import io.mockk.verify
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.api.extension.ExtendWith
import java.math.BigDecimal
import java.util.concurrent.Executors
import kotlin.test.assertEquals

@ExtendWith(MockKExtension::class)
@EnabledIfEnvironmentVariable(named = "RUN_TESTCONTAINERS", matches = "1|true|yes")
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
        val topicName = "test-business-events-1"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$topicName', '$snsMessageString')"

        every {
            snsProducer.send(topicName, any() as SNSMessage<Any>)
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
            snsProducer.send(topicName, any() as SNSMessage<Any>)
        }
    }

    @Test
    fun `format_v2 - without type - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V2)
        val snsMessage = SNSMessageMother.build()
        val snsMessageString = defaultMapper.writeValueAsString(snsMessage)
        val topicName = "test-business-events-2"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$topicName', '$snsMessageString')"

        every {
            snsProducer.send(topicName, any() as SNSMessage<Any>)
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
            snsProducer.send(topicName, any() as SNSMessage<Any>)
        }
    }

    @Test
    fun `format_v1 - SNS - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V1)
        val snsMessage = SNSMessageMother.build()
        val snsMessageString = defaultMapper.writeValueAsString(snsMessage)
        val prefix = "SNS|test-business-events-3"
        val topicName = "test-business-events-3"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$prefix', '$snsMessageString')"

        every {
            snsProducer.send(topicName, any() as SNSMessage<Any>)
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
            snsProducer.send(topicName, any() as SNSMessage<Any>)
        }
    }

    @Test
    fun `format_v2 - SNS - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V2)
        val snsMessage = SNSMessageMother.build()
        val snsMessageString = defaultMapper.writeValueAsString(snsMessage)
        val prefix = "SNS|test-business-events-4"
        val topicName = "test-business-events-4"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$prefix', '$snsMessageString')"

        every {
            snsProducer.send(topicName, any() as SNSMessage<Any>)
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
            snsProducer.send(topicName, any() as SNSMessage<Any>)
        }
    }

    @Test
    fun `format_v1 - SQS - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V1)
        val payload = PayloadMother.build()
        val sqsPayloadString = defaultMapper.writeValueAsString(payload)
        val prefix = "SQS|test-business-events-5"
        val queueName = "test-business-events-5"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$prefix', '$sqsPayloadString')"
        val mapSlot = CapturingSlot<Map<String, Any>>()

        every {
            sqsProducer.send(queueName, capture(mapSlot))
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
            sqsProducer.send(queueName, any())
        }

        val map = mapSlot.captured
        assertEquals(payload.name, map["name"])
        assertEquals(payload.price, (map["price"] as BigDecimal).toDouble())
        assertEquals(payload.description, map["description"])
    }

    @Test
    fun `format_v2 - SQS - read one message from slot in testing mode`() {
        // given
        slotReaderMessageProducer = buildSlotReaderProducer(FormatVersionEnum.V2)
        val payload = PayloadMother.build()
        val sqsPayloadString = defaultMapper.writeValueAsString(payload)
        val prefix = "SQS|test-business-events-6"
        val queueName = "test-business-events-6"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$prefix', '$sqsPayloadString')"
        val mapSlot = CapturingSlot<Map<String, Any>>()

        every {
            sqsProducer.send(queueName, capture(mapSlot))
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
            sqsProducer.send(queueName, any())
        }

        val map = mapSlot.captured
        assertEquals(payload.name, map["name"])
        assertEquals(payload.price, (map["price"] as BigDecimal).toDouble())
        assertEquals(payload.description, map["description"])
    }

    private fun buildSlotReaderProducer(formatVersion: FormatVersionEnum): SlotReaderMessageProducer {
        replicationConfiguration = replicationConfiguration.copy(formatVersion = formatVersion)

        return SlotReaderMessageProducer(
            postgresConfiguration,
            replicationConfiguration,
            snsProducer,
            sqsProducer,
        )
    }
}
