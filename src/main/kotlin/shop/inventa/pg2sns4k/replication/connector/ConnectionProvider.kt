package shop.inventa.pg2sns4k.replication.connector

import java.sql.Connection
import java.util.Properties

interface ConnectionProvider {
    fun getConnection(url: String, properties: Properties): Connection
}
