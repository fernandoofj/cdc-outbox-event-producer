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
import shop.inventa.pg2sns4k.aws.sns.SNSTransactionalProducer
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessageMother
import shop.inventa.pg2sns4k.common.IntegrationBase
import shop.inventa.pg2sns4k.jackson.ObjectMapperSingleton.defaultMapper
import java.util.concurrent.Executors
import kotlin.test.assertEquals

@ExtendWith(MockKExtension::class)
internal class SlotReaderSNSProducerIT : IntegrationBase() {

    private lateinit var slotReaderSNSProducer: SlotReaderSNSProducer

    @MockK
    private lateinit var snsTransactionalProducer: SNSTransactionalProducer

    @BeforeAll
    override fun setUp() {
        super.setUpBegin()

        slotReaderSNSProducer = SlotReaderSNSProducer(
            postgresConfiguration,
            replicationConfiguration,
            snsTransactionalProducer
        )
    }

    @AfterAll
    override fun tearDown() {
        super.tearDownEnd()
    }

    @Test
    fun `read one message from slot in testing mode`() {
        // given
        val snsMessage = SNSMessageMother.build()
        val snsMessageString = defaultMapper.writeValueAsString(snsMessage)
        val topicName = "test-business-events"
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, '$topicName', '$snsMessageString')"

        every {
            snsTransactionalProducer.send(topicName, snsMessage)
        } just runs

        // when
        val emitMessageResult = executeCommand(emitMessageCommand)
        assertEquals(true, emitMessageResult)

        val executorService = Executors.newSingleThreadExecutor()
        executorService.submit {
            Thread.sleep(2000)
            slotReaderSNSProducer.stopStreaming()
        }

        slotReaderSNSProducer.startStreaming()

        // then
        verify(exactly = 1) {
            snsTransactionalProducer.send(topicName, snsMessage)
        }
    }
}
