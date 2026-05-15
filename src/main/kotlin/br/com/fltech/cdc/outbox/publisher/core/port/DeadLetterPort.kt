package br.com.fltech.cdc.outbox.publisher.core.port

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent

/**
 * Hexagonal-core dead-letter port.
 *
 * The orchestrator hands a failing [OutboxEvent] (plus the last
 * exception observed) to this port when retries are exhausted. The
 * port is responsible for persisting / forwarding the event somewhere
 * an operator can recover from — typically a dedicated SQS queue, a
 * Kafka topic, or a database table.
 *
 * Contract:
 *  - `send` is the operation that "owns" the event once retries are
 *    exhausted. The orchestrator advances its source's checkpoint
 *    ONLY after `send` returns normally.
 *  - Implementations MUST raise on permanent failure; the orchestrator
 *    keeps the source held back so the next iteration retries
 *    delivery (which may now succeed) or dead-letters again.
 *  - Implementations MUST NOT pull Postgres / wal2json / broker types
 *    onto this port's signature. That is the whole point of hosting
 *    the port in `core/` — the application module must compile without
 *    a Postgres driver on the classpath.
 *
 * The legacy `br.com.fltech.cdc.outbox.publisher.deadletter.DeadLetterSink`
 * (signature `(LogSequenceNumber, MessageChange, Throwable)`) is kept
 * for the legacy `SlotReaderMessageProducer` path; an adapter in
 * `adapter.deadletter.LegacyDeadLetterPortAdapter` bridges it to this
 * port for the hexagonal path so consumers can keep their existing
 * `DeadLetterSink` bean.
 */
fun interface DeadLetterPort {
    fun send(event: OutboxEvent, cause: Throwable)
}
