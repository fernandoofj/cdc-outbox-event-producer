package br.com.fltech.outbox.publisher.replication.connector

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

class DefaultConnectionProvider : ConnectionProvider {
    override fun getConnection(url: String, properties: Properties): Connection {
        return DriverManager.getConnection(url, properties)
    }
}
