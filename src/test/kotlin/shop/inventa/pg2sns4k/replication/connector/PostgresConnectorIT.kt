package shop.inventa.pg2sns4k.replication.connector

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessageMother
import shop.inventa.pg2sns4k.common.IntegrationBase
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

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
        val snsMessageString = defaultMapper().writeValueAsString(SNSMessageMother.build())
        val emitMessageCommand =
            "SELECT pg_logical_emit_message(true, 'catalogue-collection-business-events', '$snsMessageString')"

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

    companion object {
        private fun defaultMapper(): ObjectMapper {
            val objectMapper = ObjectMapper()
            objectMapper.registerModule(JavaTimeModule())
            objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            return objectMapper
        }
    }
}
