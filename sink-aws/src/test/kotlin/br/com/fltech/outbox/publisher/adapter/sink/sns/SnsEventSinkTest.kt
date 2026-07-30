package br.com.fltech.outbox.publisher.adapter.sink.sns

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import io.awspring.cloud.sns.core.SnsTemplate
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.Instant

class SnsEventSinkTest {

    private val snsTemplate = mockk<SnsTemplate>()
    private val sink = SnsEventSink(snsTemplate)

    @Test
    fun `publish forwards payload as UTF-8 string and merges headers with routing attributes`() {
        every { snsTemplate.convertAndSend(any<String>(), any<String>(), any<Map<String, Any>>()) } just Runs

        val routing = Routing(
            scheme = "sns",
            target = "orders.events",
            attributes = mapOf("tenant" to "acme", "eventType" to "OrderCreated-attr"),
        )
        val event = OutboxEvent(
            id = "e1",
            routing = routing,
            payload = """{"order":1}""".toByteArray(Charsets.UTF_8),
            occurredAt = Instant.EPOCH,
            sourceCheckpoint = "0/1",
            headers = mapOf("eventType" to "OrderCreated", "trace-id" to "abc"),
        )

        sink.publish(routing, event)

        // routing.attributes override event.headers on key collisions
        verify(exactly = 1) {
            snsTemplate.convertAndSend(
                "orders.events",
                """{"order":1}""",
                mapOf(
                    "eventType" to "OrderCreated-attr",
                    "trace-id" to "abc",
                    "tenant" to "acme",
                ),
            )
        }
    }
}
