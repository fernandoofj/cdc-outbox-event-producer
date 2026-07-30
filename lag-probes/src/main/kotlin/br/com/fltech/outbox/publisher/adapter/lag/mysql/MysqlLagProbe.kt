package br.com.fltech.outbox.publisher.adapter.lag.mysql

import br.com.fltech.outbox.publisher.core.port.CheckpointStore
import br.com.fltech.outbox.publisher.core.port.LagProbe
import org.slf4j.LoggerFactory
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

/**
 * [LagProbe] for MySQL binlog-based row sources.
 *
 * MySQL has no equivalent to Postgres's `pg_wal_lsn_diff` — the
 * server-side replication position is `(File, Position)` pair and
 * lag has to be reconstructed by the consumer. This probe:
 *
 *  1. Queries `SHOW MASTER STATUS` for the current binlog
 *     `(file, position)` head.
 *  2. Reads the persisted checkpoint via [CheckpointStore] using the
 *     same `binlog:<serverId>` key the binlog source writes.
 *  3. When the two `File` values match, returns
 *     `serverPosition - checkpointPosition` as the lag in bytes.
 *  4. When the `File` values differ — the binlog rotated between
 *     ack and probe — returns `null`. A cross-file lag estimate would
 *     require summing the sizes of the intervening binlog files
 *     (one `SHOW BINARY LOGS` call), which is operator-friendly but
 *     also a foot-gun: the rotated files may already be purged when
 *     we query, producing wildly wrong values. The conservative
 *     "null + INFO log once" path keeps dashboards honest. Operators
 *     who need cross-file lag can compute it from `binlog-bytes`
 *     totals at the alerting layer.
 *
 * Same defensive contract as [br.com.fltech.outbox.publisher.adapter.lag.postgres.PostgresLagProbe]:
 * any SQL exception → WARN log + `null` return.
 */
class MysqlLagProbe(
    private val dataSource: DataSource,
    private val checkpointStore: CheckpointStore,
    private val serverId: Long,
) : LagProbe {
    override val sourceLabel: String = SOURCE_LABEL

    /**
     * Gate so the "binlog rotated since last checkpoint" message is
     * logged at INFO exactly once per rotation transition — repeating
     * it at every probe tick would flood the log. Reset on
     * `lagBytes()` calls that successfully match files, so a future
     * rotation re-arms the message.
     */
    private val rotationWarned = AtomicBoolean(false)

    // ReturnCount=7 by design — each branch represents a distinct
    // failure mode that warrants its own log line. Collapsing them
    // into a state machine would obscure the readable narrative for
    // a probe whose value is in the diagnostic logging itself.
    @Suppress("ReturnCount")
    override fun lagBytes(): Long? {
        val (checkpointFile, checkpointPosition) = loadCheckpoint() ?: return null

        val head =
            try {
                queryMasterStatus()
            } catch (e: SQLException) {
                logger.warn(
                    "MysqlLagProbe: SHOW MASTER STATUS failed ({}); returning null.",
                    e.javaClass.simpleName, e,
                )
                return null
            } ?: return null

        val (serverFile, serverPosition) = head
        if (serverFile != checkpointFile) {
            // Cross-file lag: see class KDoc for why we don't estimate.
            if (rotationWarned.compareAndSet(false, true)) {
                logger.info(
                    "MysqlLagProbe: binlog rotated since last ack " +
                        "(checkpoint file='{}' server file='{}'); reporting null until next ack lands in the new file.",
                    checkpointFile,
                    serverFile,
                )
            }
            return null
        }
        // Files match — re-arm rotation warning for the next rotation.
        rotationWarned.set(false)
        val lag = serverPosition - checkpointPosition
        return if (lag < 0L) {
            // Defensive: the persisted checkpoint sits *ahead* of the
            // server's current position. Shouldn't happen in normal
            // operation (we ack what the server emitted), but a manual
            // PURGE BINARY LOGS or a clock-skewed restart could
            // produce it. Surface and treat as "no data" rather than
            // report a negative lag.
            logger.warn(
                "MysqlLagProbe: checkpoint position {} is ahead of server position {} in file {}; returning null.",
                checkpointPosition,
                serverPosition,
                serverFile,
            )
            null
        } else {
            lag
        }
    }

    /**
     * Reads the persisted checkpoint, parses it, and returns the
     * `(file, position)` pair. Returns `null` when there is no
     * anchor to measure lag from — either the source hasn't acked
     * anything yet (normal startup state, kept silent) or the
     * persisted value did not parse (logged WARN) or the
     * `CheckpointStore.load` raised (logged WARN). Extracted out
     * of [lagBytes] so the diagnostic narrative there stays
     * focused on the upstream comparison.
     */
    @Suppress("ReturnCount")
    private fun loadCheckpoint(): Pair<String, Long>? {
        val persisted =
            try {
                checkpointStore.load(checkpointKey())
            } catch (e: Exception) {
                logger.warn(
                    "MysqlLagProbe: CheckpointStore.load failed for key={} ({}); returning null.",
                    checkpointKey(), e.javaClass.simpleName, e,
                )
                return null
            } ?: return null
        val parsed = parseCheckpoint(persisted)
        if (parsed == null) {
            logger.warn(
                "MysqlLagProbe: persisted checkpoint '{}' did not parse as <filename>:<position>; returning null.",
                persisted,
            )
            return null
        }
        return parsed
    }

    private fun queryMasterStatus(): Pair<String, Long>? =
        dataSource.connection.use { conn ->
            conn.prepareStatement(MASTER_STATUS_QUERY).use { stmt ->
                stmt.executeQuery().use { rs ->
                    readMasterStatusRow(rs)
                }
            }
        }

    private fun readMasterStatusRow(rs: java.sql.ResultSet): Pair<String, Long>? {
        if (!rs.next()) {
            logger.warn(
                "MysqlLagProbe: SHOW MASTER STATUS returned no rows; " +
                    "is the server configured as a binlog master?",
            )
            return null
        }
        val file = rs.getString("File")
        val position = rs.getLong("Position")
        return if (file.isNullOrBlank()) {
            logger.warn("MysqlLagProbe: SHOW MASTER STATUS returned blank file; returning null.")
            null
        } else {
            file to position
        }
    }

    // ReturnCount=3 mirrors the parser in MySqlBinlogRowChangeSource;
    // keeping the shape identical so an edit in one is trivially
    // mirrored in the other.
    @Suppress("ReturnCount")
    private fun parseCheckpoint(raw: String): Pair<String, Long>? {
        val idx = raw.lastIndexOf(':')
        if (idx <= 0 || idx == raw.length - 1) return null
        val file = raw.substring(0, idx)
        val position = raw.substring(idx + 1).toLongOrNull() ?: return null
        return file to position
    }

    private fun checkpointKey(): String = "binlog:$serverId"

    companion object {
        private val logger = LoggerFactory.getLogger(MysqlLagProbe::class.java)
        const val SOURCE_LABEL = "mysql"
        internal const val MASTER_STATUS_QUERY = "SHOW MASTER STATUS"
    }
}
