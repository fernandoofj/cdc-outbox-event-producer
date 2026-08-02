package br.com.fltech.outbox.publisher.common

import br.com.fltech.outbox.publisher.e2e.support.E2EContainers
import br.com.fltech.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.outbox.publisher.replication.connector.DefaultConnectionProvider
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.Connection
import java.util.Properties

/**
 * Base for the legacy Postgres-backed ITs. Boots its own
 * [E2EContainers.newPostgres] instead of depending on a docker-compose
 * Postgres already running on a fixed host port — a hard-coded
 * `localhost:5432` breaks the moment anything else on the machine (this
 * project's own `docker-compose.yml`, or an unrelated project) is already
 * bound to that port, and unlike every other `*IT.kt` in this codebase it
 * offered no dynamic-port alternative.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class IntegrationBase {
    private lateinit var postgres: PostgreSQLContainer<Nothing>
    private lateinit var auxiliarConnection: Connection

    protected lateinit var postgresConfiguration: PostgresConfiguration
    protected lateinit var replicationConfiguration: ReplicationConfiguration

    abstract fun setUp()

    abstract fun tearDown()

    protected fun setUpBegin() {
        postgres = E2EContainers.newPostgres()
        postgres.start()

        postgresConfiguration =
            PostgresConfiguration(
                host = postgres.host,
                port = postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT).toString(),
                database = postgres.databaseName,
                username = postgres.username,
                password = postgres.password,
            )
        replicationConfiguration = ReplicationConfiguration(slotName = SLOT_NAME)

        auxiliarConnection =
            createConnection(postgresConfiguration.getUrl(), postgresConfiguration.getQueryConnectionProperties())

        val createSlotCommand =
            "SELECT pg_create_logical_replication_slot(" +
                "'${replicationConfiguration.slotName}'," +
                "'${replicationConfiguration.outputPlugin}')"

        executeCommand(createSlotCommand)
    }

    protected fun tearDownEnd() {
        try {
            val dropSlotCommand = "SELECT pg_drop_replication_slot('${replicationConfiguration.slotName}')"
            executeCommand(dropSlotCommand)
            auxiliarConnection.close()
        } finally {
            // The container is dedicated to this test class (Round 24), so a
            // failure above must not leak it — Ryuk eventually reaps orphans,
            // but stop() here is immediate and doesn't depend on that safety net.
            postgres.stop()
        }
    }

    protected fun executeCommand(command: String): Boolean {
        val statement = auxiliarConnection.createStatement()
        val isSuccess = statement.execute(command)
        statement.close()
        return isSuccess
    }

    companion object {
        private const val SLOT_NAME = "catalog_slot"

        private fun createConnection(
            url: String,
            properties: Properties,
        ): Connection = DefaultConnectionProvider().getConnection(url, properties)
    }
}
