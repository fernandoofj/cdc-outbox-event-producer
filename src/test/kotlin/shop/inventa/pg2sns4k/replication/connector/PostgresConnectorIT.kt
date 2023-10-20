package shop.inventa.pg2sns4k.replication.connector

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import shop.inventa.pg2sns4k.common.IntegrationBase
import java.sql.Connection
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

internal class PostgresConnectorIT : IntegrationBase() {

    private lateinit var postgresConnector: PostgresConnector
    private lateinit var auxiliarConnection: Connection

    @BeforeAll
    override fun setUp() {

        super.setUpInternal()

        auxiliarConnection =
            createConnection(postgresConfiguration.getUrl(), postgresConfiguration.getQueryConnectionProperties())

        postgresConnector =
            PostgresConnector(postgresConfiguration, replicationConfiguration, DefaultConnectionProvider())
    }

    @AfterAll
    override fun tearDown() {
        auxiliarConnection.close()
        postgresConnector.close()

        super.tearDownInternal()
    }

    @Test
    fun `read pending data`() {
        // given
        @Suppress("MaxLineLength")
        val emitMessageCommand = "SELECT pg_logical_emit_message(true, 'catalogue-collection-business-events', '{\"headers\":{\"eventType\":\"eventType\",\"eventTimestamp\":\"2023-10-18T15:37:47.539787\"},\"body\":{\"eventUUID\":\"2dcffe9d-d191-4155-ab11-4a3b4125f3a9\",\"eventType\":\"eventType\",\"domainId\":\"domainId\",\"domain\":\"domain\",\"eventTimestamp\":\"2023-10-18T15:37:47.539787\",\"payload\":{\"uuid\":\"4ee8b1fb-f002-4d52-b0bc-ec840786f3cb\",\"shopifyCollectionId\":null,\"name\":\"Collection Cool\",\"slug\":\"collection-cool\",\"description\":\"Collection Cool Description\",\"status\":\"status\",\"disjunctive\":false,\"createdAt\":\"2023-10-18T15:37:47.540245\",\"updatedAt\":\"2023-10-18T15:37:47.540251\"}}}')"

        // when
        val beforeLSN = postgresConnector.currentLSN()
        val beforeCommandResultBytes = postgresConnector.readPending()
        val result = executeCommand(emitMessageCommand)
        val afterCommandResultBytes = postgresConnector.readPending()
        val afterLSN = postgresConnector.currentLSN()

        // then
        assertEquals(true, result)
        assertNull(beforeCommandResultBytes)
        assertNotNull(afterCommandResultBytes)
        assertNotEquals(beforeLSN, afterLSN)
    }

    private fun executeCommand(command: String): Boolean {
        val statement = auxiliarConnection.createStatement()
        val isSuccess = statement.execute(command)
        statement.close()
        return isSuccess
    }

    private fun createConnection(url: String, properties: Properties) =
        DefaultConnectionProvider().getConnection(url, properties)
}
