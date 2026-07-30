package br.com.fltech.outbox.publisher.adapter.sink.registry

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSink
import br.com.fltech.outbox.publisher.core.port.NoSinkForSchemeException
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNull

class DefaultEventSinkRegistryTest {

    private val sns = mockk<EventSink>(relaxed = true)
    private val sqs = mockk<EventSink>(relaxed = true)
    private val kafka = mockk<EventSink>(relaxed = true)

    private val registry = DefaultEventSinkRegistry(
        mapOf("sns" to sns, "sqs" to sqs, "kafka" to kafka),
    )

    @Test
    fun `resolve is case-insensitive on the scheme`() {
        assertEquals(sns, registry.resolve("sns"))
        assertEquals(sns, registry.resolve("SNS"))
        assertEquals(kafka, registry.resolve("Kafka"))
    }

    @Test
    fun `resolve returns null for unknown schemes`() {
        assertNull(registry.resolve("nats"))
    }

    @Test
    fun `publish delegates to the resolved sink and passes the routing through`() {
        val routing = Routing("kafka", "orders.events", mapOf("partition-key" to "domain-1"))
        val event = OutboxEvent("e1", routing, ByteArray(0), Instant.EPOCH, "ck")
        registry.publish(routing, event)
        verify(exactly = 1) { kafka.publish(routing, event) }
        verify(exactly = 0) { sns.publish(any(), any()) }
        verify(exactly = 0) { sqs.publish(any(), any()) }
    }

    @Test
    fun `publish raises NoSinkForSchemeException with the known schemes set`() {
        val routing = Routing("nats", "x")
        val event = OutboxEvent("e", routing, ByteArray(0), Instant.EPOCH, "ck")
        val exception = assertThrows<NoSinkForSchemeException> { registry.publish(routing, event) }
        assertEquals("nats", exception.scheme)
        assertEquals(setOf("sns", "sqs", "kafka"), exception.knownSchemes)
    }

    @Test
    fun `knownSchemes reports the registered keys (lower-cased)`() {
        val r = DefaultEventSinkRegistry(mapOf("SNS" to sns, "Kafka" to kafka))
        assertEquals(setOf("sns", "kafka"), r.knownSchemes())
    }
}
