package br.com.fltech.cdc.outbox.publisher.replication.connector

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

/**
 * Connection-provider backed by HikariCP for ordinary query connections.
 *
 * Postgres logical-replication connections are negotiated with the
 * `replication=database` startup parameter and run a different wire protocol;
 * they cannot share a JDBC pool with regular query connections. This provider
 * detects that property and falls back to a one-shot [DriverManager] connection
 * for replication mode, while pooling everything else.
 *
 * One pool is created per `(url, properties)` pair seen at runtime. In
 * practice the slot reader uses one pool for the query connection and the
 * driver-manager path for the streaming connection.
 */
class HikariCPConnectionProvider(
    private val poolConfig: PoolConfig = PoolConfig(),
) : ConnectionProvider {

    private val pools = ConcurrentHashMap<PoolKey, HikariDataSource>()

    override fun getConnection(url: String, properties: Properties): Connection {
        if (isReplicationConnection(properties)) {
            return DriverManager.getConnection(url, properties)
        }
        val key = PoolKey(url, properties)
        val pool = pools.computeIfAbsent(key) { buildPool(url, properties) }
        return pool.connection
    }

    /** Closes all underlying pools. Idempotent. */
    fun close() {
        pools.values.forEach { runCatching { it.close() } }
        pools.clear()
    }

    private fun isReplicationConnection(properties: Properties): Boolean =
        properties.getProperty(REPLICATION_PROPERTY) != null

    private fun buildPool(url: String, properties: Properties): HikariDataSource {
        // Split the JDBC properties into Hikari's typed setters (user/password)
        // and a passthrough map for the rest. Forwarding the whole Properties
        // bag as `dataSourceProperties` works today but mis-uses the field,
        // which is meant for driver-specific extras (TCP keepalive, SSL knobs,
        // etc.), not core credentials.
        val driverProperties = Properties().apply { putAll(properties) }
        val configuredUser = driverProperties.remove(PG_USER)?.toString()
        val configuredPassword = driverProperties.remove(PG_PASSWORD)?.toString()

        val config = HikariConfig().apply {
            jdbcUrl = url
            configuredUser?.let { username = it }
            configuredPassword?.let { password = it }
            dataSourceProperties = driverProperties
            maximumPoolSize = poolConfig.maximumPoolSize
            minimumIdle = poolConfig.minimumIdle
            connectionTimeout = poolConfig.connectionTimeout.toMillis()
            validationTimeout = poolConfig.validationTimeout.toMillis()
            idleTimeout = poolConfig.idleTimeout.toMillis()
            maxLifetime = poolConfig.maxLifetime.toMillis()
            leakDetectionThreshold = poolConfig.leakDetectionThreshold.toMillis()
            poolName = poolConfig.poolName
            isAutoCommit = poolConfig.autoCommit
        }
        return HikariDataSource(config)
    }

    private data class PoolKey(val url: String, val properties: Properties)

    /**
     * Pool tuning. Defaults are conservative for the single-consumer pattern
     * used by [br.com.fltech.cdc.outbox.publisher.workflow.SlotReaderMessageProducer]:
     * the query connection is held briefly to flush LSN or fetch
     * `pg_current_wal_lsn()`, so a tiny pool with strict timeouts is enough.
     */
    data class PoolConfig(
        val maximumPoolSize: Int = DEFAULT_MAX_POOL_SIZE,
        val minimumIdle: Int = DEFAULT_MIN_IDLE,
        val connectionTimeout: Duration = Duration.ofSeconds(DEFAULT_CONNECTION_TIMEOUT_SECONDS),
        val validationTimeout: Duration = Duration.ofSeconds(DEFAULT_VALIDATION_TIMEOUT_SECONDS),
        val idleTimeout: Duration = Duration.ofMinutes(DEFAULT_IDLE_TIMEOUT_MINUTES),
        val maxLifetime: Duration = Duration.ofMinutes(DEFAULT_MAX_LIFETIME_MINUTES),
        val leakDetectionThreshold: Duration = Duration.ofSeconds(DEFAULT_LEAK_DETECTION_SECONDS),
        val poolName: String = DEFAULT_POOL_NAME,
        val autoCommit: Boolean = true,
    )

    companion object {
        const val REPLICATION_PROPERTY = "replication"
        const val PG_USER = "user"
        const val PG_PASSWORD = "password"
        const val DEFAULT_MAX_POOL_SIZE = 2
        const val DEFAULT_MIN_IDLE = 0
        const val DEFAULT_CONNECTION_TIMEOUT_SECONDS = 5L
        const val DEFAULT_VALIDATION_TIMEOUT_SECONDS = 3L
        const val DEFAULT_IDLE_TIMEOUT_MINUTES = 5L
        const val DEFAULT_MAX_LIFETIME_MINUTES = 30L
        const val DEFAULT_LEAK_DETECTION_SECONDS = 30L
        const val DEFAULT_POOL_NAME = "cdc-outbox-query-pool"
    }
}
