package br.com.fltech.outbox.publisher.adapter.lag.postgres

import br.com.fltech.outbox.publisher.core.port.LagProbe
import br.com.fltech.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.outbox.publisher.replication.connector.ConnectionProvider
import org.slf4j.LoggerFactory
import java.sql.SQLException

/**
 * [LagProbe] for Postgres logical-replication slots.
 *
 * Computes lag as the byte distance between the current WAL insert
 * position and the slot's confirmed flush LSN:
 *
 * ```
 * SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
 * FROM pg_replication_slots
 * WHERE slot_name = ?
 * ```
 *
 * `pg_wal_lsn_diff` returns a `NUMERIC` whose value is the number of
 * bytes between the two LSNs — exactly what operators want to chart
 * "how stale is the consumer". When `confirmed_flush_lsn` is `NULL`
 * (slot defined but never streamed from) or the slot row is missing
 * (slot dropped behind the producer's back) the probe returns `null`
 * and emits a WARN log so the operator sees the gap rather than a
 * spuriously-flat gauge.
 *
 * Uses the same [ConnectionProvider] as the rest of the adapter so
 * the query lands on the same Hikari pool that backs query-mode
 * connections — replication-mode credentials are not needed for the
 * `pg_replication_slots` view.
 *
 * Defensive on every SQL exception: log WARN, return `null`. The
 * scheduler keeps the gauge stable across transient outages.
 */
class PostgresLagProbe(
    private val postgresConfiguration: PostgresConfiguration,
    private val connectionProvider: ConnectionProvider,
    private val slotName: String,
) : LagProbe {
    override val sourceLabel: String = SOURCE_LABEL

    override fun lagBytes(): Long? =
        try {
            queryLagBytes()
        } catch (e: SQLException) {
            logger.warn(
                "PostgresLagProbe: SQL query failed for slot '{}' ({}); returning null.",
                slotName,
                e.javaClass.simpleName,
                e,
            )
            null
        }

    /**
     * Runs the lag query against the configured connection provider.
     * Split out of [lagBytes] so the try/catch stays flat and detekt's
     * `NestedBlockDepth` rule is satisfied; behaviour is unchanged.
     */
    private fun queryLagBytes(): Long? =
        connectionProvider.getConnection(
            postgresConfiguration.getUrl(),
            postgresConfiguration.getQueryConnectionProperties(),
        ).use { conn ->
            conn.prepareStatement(LAG_QUERY).use { stmt ->
                stmt.setString(1, slotName)
                stmt.executeQuery().use { rs -> readLagRow(rs) }
            }
        }

    private fun readLagRow(rs: java.sql.ResultSet): Long? {
        if (!rs.next()) {
            logger.warn(
                "PostgresLagProbe: slot '{}' not found in pg_replication_slots; returning null.",
                slotName,
            )
            return null
        }
        val lag = rs.getLong(1)
        // ResultSet.getLong returns 0 for SQL NULL — discriminate
        // explicitly so we don't report "no data" as a real zero.
        return if (rs.wasNull()) {
            logger.warn(
                "PostgresLagProbe: confirmed_flush_lsn is NULL for slot '{}'; returning null.",
                slotName,
            )
            null
        } else {
            lag
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresLagProbe::class.java)
        const val SOURCE_LABEL = "postgres"

        internal const val LAG_QUERY =
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) " +
                "FROM pg_replication_slots WHERE slot_name = ?"
    }
}
