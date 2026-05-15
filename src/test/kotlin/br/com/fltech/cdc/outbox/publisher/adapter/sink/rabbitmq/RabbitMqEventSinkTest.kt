package br.com.fltech.cdc.outbox.publisher.adapter.sink.rabbitmq

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.domain.Routing
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.core.RabbitTemplate
import java.time.Instant
import kotlin.test.assertEquals

class RabbitMqEventSinkTest {

    @Test
    fun `target with slash splits into exchange and routing key`() {
        val template = mockk<RabbitTemplate>()
        val captured = slot<Message>()
        every { template.send(eq("orders-exchange"), eq("orders.created"), capture(captured)) } just Runs
        val sink = RabbitMqEventSink(template)

        val routing = Routing("amqp", "orders-exchange/orders.created", mapOf("env" to "prod"))
        val event = OutboxEvent(
            id = "domain-1",
            routing = routing,
            payload = "hello".toByteArray(),
            occurredAt = Instant.parse("2026-05-14T00:00:00Z"),
            sourceCheckpoint = "ck",
            headers = mapOf("trace-id" to "abc"),
        )

        sink.publish(routing, event)

        verify(exactly = 1) { template.send("orders-exchange", "orders.created", any<Message>()) }
        val m = captured.captured
        assertEquals("hello", String(m.body, Charsets.UTF_8))
        assertEquals("domain-1", m.messageProperties.messageId)
        assertEquals("abc", m.messageProperties.headers["trace-id"])
        assertEquals("prod", m.messageProperties.headers["env"])
    }

    @Test
    fun `target without slash publishes through the default exchange (empty string)`() {
        val template = mockk<RabbitTemplate>()
        every { template.send(eq(""), eq("orders-queue"), any<Message>()) } just Runs
        val sink = RabbitMqEventSink(template)

        val routing = Routing("amqp", "orders-queue")
        val event = OutboxEvent("e", routing, ByteArray(0), Instant.EPOCH, "ck")

        sink.publish(routing, event)

        verify(exactly = 1) { template.send("", "orders-queue", any<Message>()) }
    }
}
