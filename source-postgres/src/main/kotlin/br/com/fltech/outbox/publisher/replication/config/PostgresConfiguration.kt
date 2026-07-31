package br.com.fltech.outbox.publisher.replication.config

import org.postgresql.PGProperty
import java.util.Properties

data class PostgresConfiguration(
    val host: String,
    val port: String = DEFAULT_PORT,
    val database: String,
    val username: String,
    val password: String,
    val sslMode: String = DEFAULT_SSL_MODE,
    val pathToRootCert: String? = null,
    val sslPassword: String? = null,
    val pathToSslKey: String? = null,
    val pathToSslCert: String? = null,
) {
    fun getReplicationProperties(): Properties {
        val properties: Properties = getQueryConnectionProperties()
        PGProperty.PREFER_QUERY_MODE.set(properties, PREFER_QUERY_MODE)
        PGProperty.REPLICATION.set(properties, DEFAULT_REPLICATION)
        return properties
    }

    fun getQueryConnectionProperties(): Properties {
        val properties: Properties = Properties()
        PGProperty.USER.set(properties, username)
        PGProperty.PASSWORD.set(properties, password)
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, MIN_SERVER_VERSION)
        PGProperty.SSL_MODE.set(properties, sslMode)
        PGProperty.SSL_ROOT_CERT.set(properties, pathToRootCert)
        PGProperty.SSL_CERT.set(properties, pathToSslCert)
        PGProperty.SSL_PASSWORD.set(properties, sslPassword)
        PGProperty.SSL_KEY.set(properties, pathToSslKey)
        return properties
    }

    fun getUrl() = "jdbc:postgresql://$host:$port/$database"

    companion object {
        const val DEFAULT_PORT: String = "5432"
        const val MIN_SERVER_VERSION: String = "14"
        const val DEFAULT_SSL_MODE: String = "disable"
        const val DEFAULT_REPLICATION = "database"
        const val PREFER_QUERY_MODE = "simple"
    }
}
