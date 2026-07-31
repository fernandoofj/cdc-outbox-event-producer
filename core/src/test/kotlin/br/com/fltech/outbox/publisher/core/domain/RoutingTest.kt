package br.com.fltech.outbox.publisher.core.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class RoutingTest {
    @Test
    fun `parse splits scheme and target on the first --colon-slash-slash-- and lower-cases the scheme`() {
        val r = Routing.parse("Kafka://orders.events")
        assertEquals("kafka", r.scheme)
        assertEquals("orders.events", r.target)
    }

    @Test
    fun `parse rejects missing or empty scheme`() {
        assertThrows<IllegalArgumentException> { Routing.parse("orders.events") }
        assertThrows<IllegalArgumentException> { Routing.parse("://orders.events") }
    }

    @Test
    fun `parsePrefix recognises URI form, legacy pipe form, and bare prefix`() {
        assertEquals(Routing("sns", "orders.events"), Routing.parsePrefix("sns://orders.events"))
        assertEquals(Routing("kafka", "orders/cluster-a"), Routing.parsePrefix("kafka://orders/cluster-a"))
        assertEquals(Routing("sns", "orders.events"), Routing.parsePrefix("SNS|orders.events"))
        assertEquals(Routing("sqs", "orders-queue"), Routing.parsePrefix("SQS|orders-queue"))
        // Bare prefix defaults to SNS (matches the legacy protocol).
        assertEquals(Routing("sns", "orders.events"), Routing.parsePrefix("orders.events"))
    }

    @Test
    fun `asUri round-trips with parse`() {
        val r = Routing("amqp", "exchange/routingKey")
        assertEquals(r, Routing.parse(r.asUri()))
    }

    @Test
    fun `init rejects blank scheme, blank target, and uppercase scheme`() {
        assertThrows<IllegalArgumentException> { Routing("", "x") }
        assertThrows<IllegalArgumentException> { Routing("sns", "") }
        assertThrows<IllegalArgumentException> { Routing("SNS", "x") }
    }
}
