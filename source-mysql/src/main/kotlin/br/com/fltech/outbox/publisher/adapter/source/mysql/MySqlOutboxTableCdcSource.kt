package br.com.fltech.outbox.publisher.adapter.source.mysql

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.port.CdcSource
import org.slf4j.LoggerFactory
import java.sql.Connection
import javax.sql.DataSource

/**
 * MySQL [CdcSource] that polls a dedicated outbox table.
 *
 * MySQL has no equivalent to Postgres's `pg_logical_emit_message`, so
 * the outbox pattern there is a real table:
 *
 * ```
 * CREATE TABLE outbox_events (
 *   id BIGINT AUTO_INCREMENT PRIMARY KEY,
 *   prefix VARCHAR(255) NOT NULL,
 *   payload LONGTEXT NOT NULL,
 *   headers JSON NULL,
 *   created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 *   published_at TIMESTAMP(6) NULL,
 *   INDEX idx_outbox_unpublished (published_at, id)
 * ) ENGINE = InnoDB;
 * ```
 *
 * Application writers `INSERT` into this table inside the same
 * transaction as the business write, with `prefix` carrying the
 * `Routing.asUri()` for the event (e.g. `sns://orders.events`).
 *
 * This adapter uses MySQL 8's `SELECT ... FOR UPDATE SKIP LOCKED`
 * idiom: each `poll()` opens a short transaction, grabs the lowest
 * unpublished row with a row lock, marks it as published, and
 * returns the [OutboxEvent]. `ack(event)` commits the
 * `published_at = NOW()` UPDATE so the row stays in the table for
 * audit but is never seen again by `poll`.
 *
 * Wave 5 will add the row-level binlog reader as a second adapter for
 * high-throughput workloads where polling overhead matters. The poller
 * variant is shipped first because the operational surface is
 * dramatically smaller (no GTID setup, no `ROW` binlog format).
 */
class MySqlOutboxTableCdcSource(
    private val dataSource: DataSource,
    private val tableName: String = DEFAULT_TABLE,
    private val batchSize: Int = DEFAULT_BATCH_SIZE,
) : CdcSource {
    init {
        // Defence in depth: the table name is interpolated directly into
        // the SQL (PreparedStatement doesn't allow parameterised
        // identifiers). Restrict to a conservative identifier shape so a
        // hostile or careless `cdc.outbox.dead-letter.queue-name`-style
        // property cannot inject a sub-query. MySQL identifiers can
        // technically be wider, but everything we ship is plain ASCII
        // snake_case anyway.
        require(tableName.matches(IDENTIFIER_REGEX)) {
            "tableName must match $IDENTIFIER_REGEX (got '$tableName')"
        }
        require(batchSize in 1..MAX_BATCH_SIZE) {
            "batchSize must be in 1..$MAX_BATCH_SIZE (got $batchSize)"
        }
    }

    // `inflightConn` and `inflightId` are mutated by `poll`/`ack`/`close`
    // exclusively from the orchestrator's single thread (see the
    // `CdcSource` KDoc on the threading contract). `@Volatile` is here
    // only so `close()` running on a different thread mid-shutdown can
    // safely read the latest write without a torn long-pointer.
    @Volatile
    private var inflightConn: Connection? = null

    @Volatile
    private var inflightId: Long? = null

    override fun open() {
        // Defensive verification only — no permanent state to hold.
        dataSource.connection.use { it.isValid(VALIDATION_TIMEOUT_SECONDS) }
    }

    override fun poll(): OutboxEvent? {
        cleanupInflight()
        val conn = dataSource.connection
        conn.autoCommit = false
        return try {
            pollLocked(conn)
        } catch (e: Exception) {
            runCatching { conn.rollback() }
            runCatching { conn.close() }
            inflightConn = null
            inflightId = null
            throw e
        }
    }

    /**
     * Runs the SKIP LOCKED prepared statement and either binds the
     * connection as the new in-flight transaction (when a row was
     * returned) or rolls back + closes it (when there is nothing
     * to publish). Extracted out of [poll] so the
     * connection/statement/result-set nesting stays at 3 levels
     * instead of 4 — same AutoCloseable ordering semantics around
     * the FOR UPDATE lock.
     */
    private fun pollLocked(conn: java.sql.Connection): OutboxEvent? =
        conn.prepareStatement(pollSql).use { stmt ->
            stmt.executeQuery().use { rs ->
                if (!rs.next()) {
                    conn.rollback()
                    conn.close()
                    null
                } else {
                    bindInflightAndProject(conn, rs)
                }
            }
        }

    private fun bindInflightAndProject(
        conn: java.sql.Connection,
        rs: java.sql.ResultSet,
    ): OutboxEvent {
        val id = rs.getLong("id")
        val prefix = rs.getString("prefix")
        val payload = rs.getString("payload")
        val createdAt = rs.getTimestamp("created_at").toInstant()
        inflightConn = conn
        inflightId = id
        return OutboxEvent(
            id = id.toString(),
            routing = Routing.parsePrefix(prefix),
            payload = payload.toByteArray(Charsets.UTF_8),
            occurredAt = createdAt,
            sourceCheckpoint = id.toString(),
        )
    }

    private val pollSql =
        """
        SELECT id, prefix, payload, headers, created_at
        FROM $tableName
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT $batchSize
        FOR UPDATE SKIP LOCKED
        """.trimIndent()

    override fun ack(event: OutboxEvent) {
        val conn =
            inflightConn ?: run {
                logger.warn("ack called with no in-flight transaction for event {}; nothing to commit", event.id)
                return
            }
        val id = inflightId ?: return
        try {
            conn.prepareStatement(
                "UPDATE $tableName SET published_at = CURRENT_TIMESTAMP(6) WHERE id = ?",
            ).use { stmt ->
                stmt.setLong(1, id)
                stmt.executeUpdate()
            }
            conn.commit()
        } finally {
            runCatching { conn.close() }
            inflightConn = null
            inflightId = null
        }
    }

    override fun close() {
        cleanupInflight()
    }

    private fun cleanupInflight() {
        inflightConn?.let { conn ->
            runCatching { conn.rollback() }
            runCatching { conn.close() }
        }
        inflightConn = null
        inflightId = null
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MySqlOutboxTableCdcSource::class.java)
        const val DEFAULT_TABLE = "outbox_events"
        const val DEFAULT_BATCH_SIZE = 1
        const val MAX_BATCH_SIZE = 1000
        const val VALIDATION_TIMEOUT_SECONDS = 3

        /** ASCII identifier shape: leading letter/underscore, then alphanumerics/underscores. */
        val IDENTIFIER_REGEX = Regex("[A-Za-z_][A-Za-z0-9_]*")
    }
}
