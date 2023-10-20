package shop.inventa.pg2sns4k.common

import com.github.dockerjava.api.command.CreateContainerCmd
import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import shop.inventa.pg2sns4k.replication.config.PostgresConfiguration
import shop.inventa.pg2sns4k.replication.config.ReplicationConfiguration
import java.util.function.Consumer

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class IntegrationBase {

    @Container
    protected val postgresContainer = PostgreSQLContainer<Nothing>(
        DockerImageName.parse("debezium/postgres:14-alpine")
            .asCompatibleSubstituteFor("postgres")
    )
    protected val replicationConfiguration = ReplicationConfiguration("catalog_slot")
    protected lateinit var postgresConfiguration: PostgresConfiguration

    protected fun setUp() {
        postgresConfiguration = postgresContainer.buildPostgresConfiguration()
        configureContainer()
        postgresContainer.start()
        Thread.sleep(2000)
    }

    protected fun tearDown() {
        postgresContainer.stop()
    }

    protected fun configureContainer() {
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

    private fun PostgreSQLContainer<Nothing>.buildPostgresConfiguration() = PostgresConfiguration(
        host = this.host,
        port = this.firstMappedPort.toString(),
        database = this.databaseName,
        username = this.username,
        password = this.password
    )
}