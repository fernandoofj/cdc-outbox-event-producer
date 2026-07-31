package br.com.fltech.outbox.publisher.adapter.sink.composite

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.EventSink
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CompositeEventSinkTest {
    private val routing = Routing("sns", "topic")
    private val event = OutboxEvent("e1", routing, ByteArray(0), Instant.EPOCH, "ck")

    @Test
    fun `requires at least one delegate`() {
        assertThrows<IllegalArgumentException> { CompositeEventSink(emptyList()) }
    }

    @Test
    fun `fanout delivers to every delegate in order`() {
        val a = mockk<EventSink>()
        every { a.publish(any(), any()) } just Runs
        val b = mockk<EventSink>()
        every { b.publish(any(), any()) } just Runs
        val c = mockk<EventSink>()
        every { c.publish(any(), any()) } just Runs

        CompositeEventSink(listOf(a, b, c)).publish(routing, event)

        verify(exactly = 1) { a.publish(routing, event) }
        verify(exactly = 1) { b.publish(routing, event) }
        verify(exactly = 1) { c.publish(routing, event) }
    }

    @Test
    fun `failFast=true (default) aborts on the first throwing delegate`() {
        val a = mockk<EventSink>()
        every { a.publish(any(), any()) } just Runs
        val b = mockk<EventSink>()
        every { b.publish(any(), any()) } throws IllegalStateException("boom")
        val c = mockk<EventSink>(relaxed = true)

        assertThrows<IllegalStateException> { CompositeEventSink(listOf(a, b, c)).publish(routing, event) }

        verify(exactly = 1) { a.publish(routing, event) }
        verify(exactly = 1) { b.publish(routing, event) }
        verify(exactly = 0) { c.publish(routing, event) }
    }

    @Test
    fun `failFast=false invokes every delegate and reports the first failure with suppressed extras`() {
        val a = mockk<EventSink>()
        every { a.publish(any(), any()) } throws RuntimeException("first")
        val b = mockk<EventSink>()
        every { b.publish(any(), any()) } just Runs
        val c = mockk<EventSink>()
        every { c.publish(any(), any()) } throws RuntimeException("third")

        val t =
            assertThrows<RuntimeException> {
                CompositeEventSink(listOf(a, b, c), failFast = false).publish(routing, event)
            }

        assertEquals("first", t.message)
        assertEquals(1, t.suppressed.size)
        assertEquals("third", t.suppressed[0].message)
        verify(exactly = 1) { a.publish(routing, event) }
        verify(exactly = 1) { b.publish(routing, event) }
        verify(exactly = 1) { c.publish(routing, event) }
        assertTrue(true)
    }
}
