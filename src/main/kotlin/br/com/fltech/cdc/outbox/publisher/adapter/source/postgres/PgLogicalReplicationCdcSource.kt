package br.com.fltech.cdc.outbox.publisher.adapter.source.postgres

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.domain.Routing
import br.com.fltech.cdc.outbox.publisher.core.port.CdcSource
import br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.connector.ConnectionProvider
import br.com.fltech.cdc.outbox.publisher.replication.connector.PostgresConnector
import br.com.fltech.cdc.outbox.publisher.replication.model.MessageChange
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserImplV1
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserImplV2
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserStrategy
import com.fasterxml.jackson.databind.ObjectMapper
import org.postgresql.replication.LogSequenceNumber
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

/**
 * [CdcSource] adapter that streams `pg_logical_emit_message` records
 * out of a Postgres logical-replication slot via [PostgresConnector].
 *
 * This is a hexagonal port-style wrapper around the existing
 * `PostgresConnector` machinery. It is *additive* — the legacy
 * `SlotReaderMessageProducer` still works as-is; the new
 * [br.com.fltech.cdc.outbox.publisher.core.application.CdcProcessor]
 * uses this adapter to publish via the [EventSink] registry.
 *
 * Only `M` (message) WAL records produce events. Insert/update/delete
 * records are silently consumed (the slot must still advance past
 * them, which the orchestrator handles via `ack` on the synthesised
 * "empty" event — but in this adapter we simply drop them and never
 * emit; the orchestrator's ack will be a no-op for any synthesised
 * checkpoint event).
 *
 * Non-blocking `poll()`: returns `null` if no message is buffered.
 * `ack(event)` advances the slot's flushed LSN to the event's
 * checkpoint, idempotent.
 */
class PgLogicalReplicationCdcSource(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration,
    private val connectionProvider: ConnectionProvider,
    objectMapper: ObjectMapper,
) : CdcSource {

    private val parser = ByteToClassParserStrategy(
        ByteToClassParserImplV1(objectMapper),
        ByteToClassParserImplV2(objectMapper),
    ).selectParser(replicationConfiguration)

    private val opened = AtomicBoolean(false)
    private var connector: PostgresConnector? = null

    override fun open() {
        if (!opened.compareAndSet(false, true)) {
            logger.debug("PgLogicalReplicationCdcSource already opened; ignoring duplicate open()")
            return
        }
        connector = PostgresConnector(postgresConfiguration, replicationConfiguration, connectionProvider)
        logger.info("PgLogicalReplicationCdcSource opened on slot '{}'", replicationConfiguration.slotName)
    }

    override fun poll(): OutboxEvent? {
        val conn = checkOpen()
        val byteBuffer = conn.readPending() ?: return null
        val fallbackLsn = conn.lastReceivedLsn()
        val changes = parser.parse(byteBuffer)
        val messageChange = changes.firstOrNull { it is MessageChange } as? MessageChange ?: return null

        val lsn = resolveLsn(messageChange.lsn, fallbackLsn)
        return OutboxEvent(
            id = lsn.asString(),
            routing = Routing.parsePrefix(messageChange.prefix),
            payload = messageChange.content.toByteArray(Charsets.UTF_8),
            occurredAt = Instant.now(),
            sourceCheckpoint = lsn.asString(),
        )
    }

    override fun ack(event: OutboxEvent) {
        val conn = checkOpen()
        val lsn = parseLsnOrNull(event.sourceCheckpoint) ?: return
        conn.setStreamLsn(lsn)
    }

    override fun close() {
        if (!opened.compareAndSet(true, false)) return
        runCatching { connector?.close() }
        connector = null
        logger.info("PgLogicalReplicationCdcSource closed for slot '{}'", replicationConfiguration.slotName)
    }

    private fun checkOpen(): PostgresConnector = connector
        ?: error("PgLogicalReplicationCdcSource must be opened before use")

    private fun resolveLsn(embedded: String?, fallback: LogSequenceNumber): LogSequenceNumber {
        if (embedded.isNullOrBlank()) return fallback
        val parsed = LogSequenceNumber.valueOf(embedded)
        return if (parsed == LogSequenceNumber.INVALID_LSN) {
            logger.warn("Embedded wal2json lsn '{}' parsed as INVALID_LSN; falling back to stream LSN {}", embedded, fallback)
            fallback
        } else {
            parsed
        }
    }

    private fun parseLsnOrNull(s: String): LogSequenceNumber? {
        val parsed = LogSequenceNumber.valueOf(s)
        return if (parsed == LogSequenceNumber.INVALID_LSN) null else parsed
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PgLogicalReplicationCdcSource::class.java)
    }
}
