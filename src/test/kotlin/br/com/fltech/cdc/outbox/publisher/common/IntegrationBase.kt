package shop.inventa.pg2sns4k.common

import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.testcontainers.junit.jupiter.Testcontainers
import shop.inventa.pg2sns4k.aws.common.AWSParamaters
import shop.inventa.pg2sns4k.replication.config.PostgresConfiguration
import shop.inventa.pg2sns4k.replication.config.ReplicationConfiguration
import shop.inventa.pg2sns4k.replication.connector.DefaultConnectionProvider
import java.io.FileInputStream
import java.sql.Connection
import java.sql.SQLException
import java.util.Properties

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class IntegrationBase {

    private lateinit var properties: Properties
    private lateinit var auxiliarConnection: Connection

    protected lateinit var postgresConfiguration: PostgresConfiguration
    protected lateinit var replicationConfiguration: ReplicationConfiguration
    protected lateinit var awsParamaters: AWSParamaters

    abstract fun setUp()
    abstract fun tearDown()

    protected fun setUpBegin() {
        properties = loadProperties()

        postgresConfiguration = properties.buildPostgresConfiguration()
        replicationConfiguration = properties.buildReplicationConfiguration()
        awsParamaters = properties.buildAwsParameters()

        auxiliarConnection =
            createConnection(postgresConfiguration.getUrl(), postgresConfiguration.getQueryConnectionProperties())

        val createSlotCommand = "SELECT pg_create_logical_replication_slot(" +
            "'${replicationConfiguration.slotName}'," +
            "'${replicationConfiguration.outputPlugin}')"

        try {
            executeCommand(createSlotCommand)
        } catch (e: SQLException) {
            when (e.sqlState) {
                ALREADY_EXISTS_SQL_STATE -> logger.info("Slot ${replicationConfiguration.slotName} already exists")
                else -> throw e
            }
        }
    }

    protected fun tearDownEnd() {
        val dropSlotCommand = "SELECT pg_drop_replication_slot('${replicationConfiguration.slotName}')"
        executeCommand(dropSlotCommand)

        properties.clear()
        auxiliarConnection.close()
    }

    protected fun executeCommand(command: String): Boolean {
        val statement = auxiliarConnection.createStatement()
        val isSuccess = statement.execute(command)
        statement.close()
        return isSuccess
    }

    companion object {

        private val logger = LoggerFactory.getLogger(IntegrationBase::class.java)
        private const val ALREADY_EXISTS_SQL_STATE = "42710"

        private fun loadProperties(): Properties {
            val properties = Properties()
            val inputStream = FileInputStream("src/test/resources/application.properties")
            properties.load(inputStream)
            return properties
        }

        private fun createConnection(url: String, properties: Properties) =
            DefaultConnectionProvider().getConnection(url, properties)

        private fun Properties.buildAwsParameters() = AWSParamaters(
            awsAccessKey = this["cloud.aws.credentials.access-key"].toString(),
            awsSecretKey = this["cloud.aws.credentials.secret-key"].toString(),
            region = this["cloud.aws.region.static"].toString(),
            localstackUrl = this["cloud.aws.localstack.url"].toString()
        )

        private fun Properties.buildPostgresConfiguration() = PostgresConfiguration(
            host = this["datasource.host"].toString(),
            port = this["datasource.port"].toString(),
            database = this["datasource.database"].toString(),
            username = this["datasource.username"].toString(),
            password = this["datasource.password"].toString()
        )

        private fun Properties.buildReplicationConfiguration() = ReplicationConfiguration(
            slotName = this["datasource.replication.slot"].toString()
        )
    }
}
