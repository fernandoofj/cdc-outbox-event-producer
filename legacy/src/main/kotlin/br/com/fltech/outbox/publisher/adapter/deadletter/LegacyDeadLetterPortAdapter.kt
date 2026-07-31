package br.com.fltech.outbox.publisher.adapter.deadletter

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.port.DeadLetterPort
import br.com.fltech.outbox.publisher.deadletter.DeadLetterSink
import br.com.fltech.outbox.publisher.replication.model.MessageChange
import org.postgresql.replication.LogSequenceNumber

/**
 * Adapter that lets the hexagonal [DeadLetterPort] reuse an existing
 * legacy [DeadLetterSink] bean.
 *
 * The legacy interface still expects a `LogSequenceNumber` + a
 * `MessageChange` (its API was designed for the Postgres-only world).
 * This adapter reconstructs both from the hexagonal [OutboxEvent]:
 *  - `LogSequenceNumber` from `event.sourceCheckpoint` (if it parses
 *    as `X/X`); otherwise `INVALID_LSN`, which the SQS envelope still
 *    renders as `0/0` — operators reading the DLQ can match by event
 *    id instead.
 *  - `MessageChange` from the routing URI + event payload.
 *
 * This adapter lives in `adapter/` (NOT `core/`) precisely because it
 * is allowed to know about Postgres types — that knowledge is exactly
 * what the hexagonal split was meant to confine to the adapter ring.
 */
class LegacyDeadLetterPortAdapter(
    private val legacy: DeadLetterSink,
) : DeadLetterPort {
    override fun send(
        event: OutboxEvent,
        cause: Throwable,
    ) {
        val lsn =
            runCatching { LogSequenceNumber.valueOf(event.sourceCheckpoint) }
                .getOrDefault(LogSequenceNumber.INVALID_LSN)
        val messageChange =
            MessageChange(
                kindInput = "message",
                transactional = true,
                prefix = event.routing.asUri(),
                content = event.payloadAsString,
                lsn = event.sourceCheckpoint,
            )
        legacy.send(lsn = lsn, messageChange = messageChange, lastException = cause)
    }
}
