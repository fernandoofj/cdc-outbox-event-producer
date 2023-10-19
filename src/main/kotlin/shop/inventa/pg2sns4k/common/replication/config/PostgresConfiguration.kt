package shop.inventa.pg2sns4k.common.replication.config

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
    val pathToSslCert: String? = null
) {
    fun getReplicationProperties(): Properties {
        val properties: Properties = getQueryConnectionProperties()
        properties[PGProperty.PREFER_QUERY_MODE.name] = PREFER_QUERY_MODE
        properties[PGProperty.REPLICATION.name] = DEFAULT_REPLICATION
        return properties
    }
    fun getQueryConnectionProperties(): Properties {
        val properties: Properties = Properties()
        properties[PGProperty.USER.name] = username
        properties[PGProperty.PASSWORD.name] = password
        properties[PGProperty.ASSUME_MIN_SERVER_VERSION.name] = MIN_SERVER_VERSION
        properties[PGProperty.SSL_MODE.name] = sslMode
        properties[PGProperty.SSL_ROOT_CERT.name] = pathToRootCert
        properties[PGProperty.SSL_CERT.name] = pathToSslCert
        properties[PGProperty.SSL_PASSWORD.name] = sslPassword
        properties[PGProperty.SSL_KEY.name] = pathToSslKey
        return properties
    }

    fun getUrl() = "jdbc:postgresql://$host:$port/$database"

    companion object {
        const val DEFAULT_PORT: String = "5432"
        const val MIN_SERVER_VERSION: String = "10.3"
        const val DEFAULT_SSL_MODE: String = "disable"
        const val DEFAULT_REPLICATION = "database"
        const val PREFER_QUERY_MODE = "simple"
    }
}
