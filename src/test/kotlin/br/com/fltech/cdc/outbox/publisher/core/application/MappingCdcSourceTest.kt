package br.com.fltech.cdc.outbox.publisher.core.application

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.domain.Routing
import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.port.MappingRules
import br.com.fltech.cdc.outbox.publisher.core.port.RowChangeSource
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MappingCdcSourceTest {

    private val now = Instant.parse("2026-05-15T00:00:00Z")
    private val rowSource = mockk<RowChangeSource>(relaxed = true)

    private fun event(checkpoint: String) = OutboxEvent(
        id = "e1",
        routing = Routing("sns", "topic"),
        payload = ByteArray(0),
        occurredAt = now,
        sourceCheckpoint = checkpoint,
    )

    private fun rowAt(checkpoint: String) = RowChange(
        op = RowChange.Op.INSERT, table = "public.orders",
        sourceCheckpoint = checkpoint, occurredAt = now,
        after = mapOf("id" to 1),
    )

    @Test
    fun `poll returns null when the underlying row source is idle`() {
        every { rowSource.poll() } returns null
        val src = MappingCdcSource(rowSource, MappingRules.EMPTY)
        assertNull(src.poll())
    }

    @Test
    fun `poll returns the mapped event and buffers the row keyed by sourceCheckpoint`() {
        val row = rowAt("0/AAAA")
        val mapped = event("0/AAAA")
        every { rowSource.poll() } returns row
        val rules = MappingRules { _ -> mapped }
        val src = MappingCdcSource(rowSource, rules)

        assertEquals(mapped, src.poll())

        // ack() should now forward to the row source.
        src.ack(mapped)
        verify(exactly = 1) { rowSource.ack(row) }
    }

    @Test
    fun `poll acks the row source and returns null when the row has no mapping`() {
        val row = rowAt("0/AAAA")
        every { rowSource.poll() } returns row
        every { rowSource.ack(row) } just Runs
        val rules = MappingRules { _ -> null }
        val src = MappingCdcSource(rowSource, rules)

        assertNull(src.poll())
        verify(exactly = 1) { rowSource.ack(row) }
    }

    @Test
    fun `ack with an unknown checkpoint is a no-op`() {
        val src = MappingCdcSource(rowSource, MappingRules.EMPTY)
        src.ack(event("0/never-seen"))
        verify(exactly = 0) { rowSource.ack(any()) }
    }

    @Test
    fun `close clears the buffer and closes the row source`() {
        every { rowSource.close() } just Runs
        val src = MappingCdcSource(rowSource, MappingRules.EMPTY)
        src.close()
        verify(exactly = 1) { rowSource.close() }
    }

    @Test
    fun `open delegates to the underlying row source`() {
        every { rowSource.open() } just Runs
        MappingCdcSource(rowSource, MappingRules.EMPTY).open()
        verify(exactly = 1) { rowSource.open() }
    }
}
