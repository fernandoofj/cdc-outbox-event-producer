package br.com.fltech.outbox.publisher.core.domain

import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class OutboxEventTest {
    @Test
    fun `equals compares the payload by content, not by array reference`() {
        val now = Instant.parse("2026-05-14T00:00:00Z")
        val a =
            OutboxEvent(
                id = "1",
                routing = Routing("sns", "topic"),
                payload = "hello".toByteArray(),
                occurredAt = now,
                sourceCheckpoint = "0/16E8198",
            )
        val b = a.copy(payload = "hello".toByteArray())
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
    }

    @Test
    fun `equals returns false when payload differs in content`() {
        val now = Instant.parse("2026-05-14T00:00:00Z")
        val a = OutboxEvent("1", Routing("sns", "t"), "hello".toByteArray(), now, "0/1")
        val b = a.copy(payload = "world".toByteArray())
        assertNotEquals(a, b)
    }

    @Test
    fun `payloadAsString decodes the byte array as UTF-8`() {
        val e =
            OutboxEvent(
                id = "1",
                routing = Routing("sns", "t"),
                payload = "café".toByteArray(Charsets.UTF_8),
                occurredAt = Instant.EPOCH,
                sourceCheckpoint = "0/1",
            )
        assertEquals("café", e.payloadAsString)
    }
}
