package br.com.fltech.outbox.publisher.core.port

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent

/**
 * Driving (input) port for the orchestrator: produces [OutboxEvent]s
 * out of a database CDC source.
 *
 * Implementations:
 *  - `adapter.source.postgres.PgLogicalReplicationCdcSource` — wraps the
 *    pgjdbc replication API and `wal2json` plugin.
 *  - `adapter.source.mysql.MySqlOutboxTableCdcSource` — polls an outbox
 *    table with `SELECT ... FOR UPDATE SKIP LOCKED`.
 *  - `adapter.source.sqlserver.SqlServerCdcSourceStub` /
 *    `adapter.source.oracle.OracleCdcSourceStub` — placeholders so the
 *    surface compiles before Wave 5 lands the real adapters.
 *
 * Lifecycle: `open()` once on start (throws if the source cannot be
 * reached); `poll()` repeatedly while the orchestrator is running;
 * `ack(event)` after the orchestrator confirms the event reached every
 * relevant sink; `close()` once on shutdown (idempotent).
 *
 * **Threading contract:** implementations are invoked from a SINGLE
 * thread per `CdcSource` instance. `poll`/`ack`/`close` must NOT be
 * called concurrently from multiple threads. Implementations that
 * carry per-call state across `poll → ack` (e.g. an open JDBC
 * connection on a `SELECT … FOR UPDATE` cursor) rely on this
 * contract. If a future orchestrator design dispatches events across
 * a worker pool, each worker must own its own `CdcSource` instance.
 *
 * Contract on `ack`:
 *  - The source MUST treat `ack(event)` as the signal to advance its
 *    own checkpoint (LSN, GTID, table cursor) past this event.
 *  - Calling `ack` out of order is permitted but the source is free to
 *    persist only the highest-checkpoint event seen so far. Sources
 *    that need strict ordering should document the constraint.
 *  - `ack` MUST be idempotent. Calling it twice for the same event is
 *    not an error.
 */
interface CdcSource : AutoCloseable {
    /**
     * Open the underlying connection / consumer. Throws on permanent
     * failure (auth, missing slot, etc.) so the orchestrator can decide
     * whether to retry.
     */
    fun open()

    /**
     * Return the next available event or `null` if the source is
     * currently idle. Non-blocking; the orchestrator runs the loop.
     */
    fun poll(): OutboxEvent?

    /**
     * Advance the source's checkpoint past [event]. Idempotent.
     */
    fun ack(event: OutboxEvent)

    /** Idempotent shutdown. */
    override fun close()
}
