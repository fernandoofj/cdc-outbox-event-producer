package br.com.fltech.outbox.publisher.adapter.sink.sqs

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import io.awspring.cloud.sqs.operations.SendResult
import io.awspring.cloud.sqs.operations.SqsSendOptions
import io.awspring.cloud.sqs.operations.SqsTemplate
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.function.Consumer

class SqsEventSinkTest {

    @Test
    fun `publish builds SqsSendOptions with queue, payload, and merged attributes`() {
        val sqsTemplate = mockk<SqsTemplate>()
        val captured = slot<Consumer<SqsSendOptions<String>>>()
        every { sqsTemplate.send(capture(captured)) } returns mockk<SendResult<String>>(relaxed = true)
        val sink = SqsEventSink(sqsTemplate)

        val routing = Routing("sqs", "orders-queue", mapOf("tenant" to "acme"))
        val event = OutboxEvent(
            id = "e1",
            routing = routing,
            payload = """{"order":1}""".toByteArray(Charsets.UTF_8),
            occurredAt = Instant.EPOCH,
            sourceCheckpoint = "0/1",
            headers = mapOf("trace-id" to "abc"),
        )

        sink.publish(routing, event)

        val options = mockk<SqsSendOptions<String>>()
        every { options.queue(any()) } returns options
        every { options.payload(any()) } returns options
        every { options.headers(any<Map<String, Any>>()) } returns options

        captured.captured.accept(options)

        verify(exactly = 1) { options.queue("orders-queue") }
        verify(exactly = 1) { options.payload("""{"order":1}""") }
        verify(exactly = 1) { options.headers(mapOf("trace-id" to "abc", "tenant" to "acme")) }
    }
}
