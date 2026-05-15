package br.com.fltech.cdc.outbox.publisher.core.application

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.port.CdcSource
import br.com.fltech.cdc.outbox.publisher.core.port.MappingRules
import br.com.fltech.cdc.outbox.publisher.core.port.RowChangeSource

/**
 * Decorator that satisfies [CdcSource] by reading from a lower-level
 * [RowChangeSource] and applying [MappingRules] on each
 * [RowChange] to produce an [OutboxEvent].
 *
 * Behaviour:
 *  - `poll()` reads at most one [RowChange] per call. If the row maps
 *    to a non-null [OutboxEvent], the row is buffered against the
 *    event's `sourceCheckpoint` and the event is returned. If the row
 *    maps to `null` (no [TableMapping][br.com.fltech.cdc.outbox.publisher.core.domain.TableMapping]
 *    matched, or the op was excluded by `capture`), the underlying
 *    row source is `ack`ed past it and `poll` returns `null` — the
 *    orchestrator will call again on the next iteration.
 *  - `ack(event)` pops the buffered row and acks the underlying source.
 *  - `close()` drops the buffer and closes the underlying source.
 *
 * Single-threaded per the [CdcSource] contract — the unsynchronised
 * `pending` map is therefore safe.
 */
class MappingCdcSource(
    private val rowSource: RowChangeSource,
    private val mappingRules: MappingRules,
) : CdcSource {

    /**
     * Maps `event.sourceCheckpoint → RowChange` so [ack] can recover
     * the original row and forward it to the underlying source. In
     * practice the orchestrator acks the event right after the publish
     * succeeds, so this map holds at most one entry under normal
     * operation; the map shape is just defensive in case a future
     * batching mode is introduced.
     */
    private val pending = mutableMapOf<String, RowChange>()

    override fun open() {
        rowSource.open()
    }

    override fun poll(): OutboxEvent? {
        val row = rowSource.poll() ?: return null
        val event = mappingRules.map(row)
        if (event == null) {
            // No mapping for this table or op excluded — advance the row
            // source past the change so it doesn't reappear. Returning
            // null prompts the orchestrator to poll again on its next
            // iteration.
            rowSource.ack(row)
            return null
        }
        pending[event.sourceCheckpoint] = row
        return event
    }

    override fun ack(event: OutboxEvent) {
        val row = pending.remove(event.sourceCheckpoint) ?: return
        rowSource.ack(row)
    }

    override fun close() {
        pending.clear()
        rowSource.close()
    }
}
