package shop.inventa.pg2sns4k.common.replication.connector

import com.github.dockerjava.api.command.CreateContainerCmd
import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import shop.inventa.pg2sns4k.common.replication.config.PostgresConfiguration
import shop.inventa.pg2sns4k.common.replication.config.ReplicationConfiguration
import java.sql.Connection
import java.util.Properties
import java.util.function.Consumer
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PostgresConnectorIT {

    @Container
    private val postgresContainer = PostgreSQLContainer<Nothing>(
        DockerImageName.parse("debezium/postgres:14-alpine")
            .asCompatibleSubstituteFor("postgres")
    )

    private lateinit var postgresConnector: PostgresConnector
    private lateinit var auxiliarConnection: Connection

    @BeforeAll
    fun setUp() {

        configureContainer()
        postgresContainer.start()

        val postgresConfiguration = buildPostgresConfiguration()
        val replicationConfiguration = ReplicationConfiguration("catalog_slot")

        Thread.sleep(2000)

        auxiliarConnection =
            createConnection(postgresConfiguration.getUrl(), postgresConfiguration.getQueryConnectionProperties())

        postgresConnector =
            PostgresConnector(postgresConfiguration, replicationConfiguration, DefaultConnectionProvider())
    }

    @AfterAll
    fun tearDown() {
        auxiliarConnection.close()
        postgresConnector.close()
        postgresContainer.stop()
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

    private fun configureContainer() {
        val containerPort = 5432
        val localPort = 5432

        val cmd =
            Consumer<CreateContainerCmd> { e: CreateContainerCmd ->
                e.withHostConfig(
                    HostConfig().withPortBindings(
                        PortBinding(
                            Ports.Binding.bindPort(localPort),
                            ExposedPort(containerPort)
                        )
                    )
                )
            }

        postgresContainer.withDatabaseName("catalogue")
        postgresContainer.withUsername("postgres")
        postgresContainer.withPassword("test")
        postgresContainer.withExposedPorts(containerPort)
        postgresContainer.withCreateContainerCmdModifier(cmd)
        postgresContainer.withCommand("postgres", "-c", "wal_level=logical")
    }

    private fun buildPostgresConfiguration() = PostgresConfiguration(
        host = postgresContainer.host,
        port = postgresContainer.firstMappedPort.toString(),
        database = postgresContainer.databaseName,
        username = postgresContainer.username,
        password = postgresContainer.password
    )

    private fun executeCommand(command: String): Boolean {
        val statement = auxiliarConnection.createStatement()
        val isSuccess = statement.execute(command)
        statement.close()
        return isSuccess
    }

    private fun createConnection(url: String, properties: Properties) =
        DefaultConnectionProvider().getConnection(url, properties)
}
