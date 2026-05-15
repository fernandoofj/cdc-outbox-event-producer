package br.com.fltech.cdc.outbox.publisher.replication.connector

import java.sql.Connection
import java.util.Properties

interface ConnectionProvider {
    fun getConnection(url: String, properties: Properties): Connection
}
