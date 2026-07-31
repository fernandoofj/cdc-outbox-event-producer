package br.com.fltech.outbox.publisher.replication.connector

import br.com.fltech.outbox.publisher.aws.sns.dto.SNSMessageMother
import br.com.fltech.outbox.publisher.common.IntegrationBase
import br.com.fltech.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

@EnabledIfEnvironmentVariable(named = "RUN_TESTCONTAINERS", matches = "1|true|yes")
internal class PostgresConnectorIT : IntegrationBase() {
    private lateinit var postgresConnector: PostgresConnector

    @BeforeAll
    override fun setUp() {
        super.setUpBegin()

        postgresConnector =
            PostgresConnector(postgresConfiguration, replicationConfiguration, DefaultConnectionProvider())
    }

    @AfterAll
    override fun tearDown() {
        postgresConnector.close()

        super.tearDownEnd()
    }

    @Test
    fun `read pending data`() {
        // given
        val snsMessageString = defaultMapper.writeValueAsString(SNSMessageMother.build())
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, 'test-business-events', '$snsMessageString')"

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
}
