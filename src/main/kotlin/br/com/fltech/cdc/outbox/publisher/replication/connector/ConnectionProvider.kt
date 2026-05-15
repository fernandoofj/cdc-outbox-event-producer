package br.com.fltech.cdc.outbox.publisher.replication.connector

import java.sql.Connection
import java.util.Properties

/**
 * Provides JDBC [Connection] instances to [PostgresConnector].
 *
 * Extends [AutoCloseable] so providers that own resources (e.g. HikariCP
 * pools) can release them when the Spring context closes. The default
 * implementation is a no-op for providers that don't need cleanup
 * (e.g. [DefaultConnectionProvider]).
 */
interface ConnectionProvider : AutoCloseable {
    fun getConnection(url: String, properties: Properties): Connection

    override fun close() {
        // no-op default — providers that own pools must override.
    }
}
