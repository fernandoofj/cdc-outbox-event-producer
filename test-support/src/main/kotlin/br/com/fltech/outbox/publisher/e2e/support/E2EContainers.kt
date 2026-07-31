package br.com.fltech.outbox.publisher.e2e.support

import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service.SNS
import org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS
import org.testcontainers.utility.DockerImageName

/**
 * Container factories shared by the E2E tests.
 *
 * Centralising the Testcontainers ceremony in one place keeps each test
 * focused on its assertion. The image tags, command-line args, and
 * default credentials mirror what the existing
 * `AtLeastOnceDeliveryIT` uses so the OrbStack gotchas
 * (`DOCKER_API_VERSION=1.43` propagated from the shell via
 * `build.gradle.kts`) stay consistent. Each factory returns a fresh
 * not-yet-started container; callers own the start/stop lifecycle and
 * MUST close the container in `@AfterAll`.
 *
 * Why factories (not pre-built fields): JUnit's lifecycle creates each
 * test class instance via reflection; sharing a `companion object`
 * container across tests works for serial runs but breaks the principle
 * of test isolation. Each test gets its own pair of containers.
 */
object E2EContainers {
    /**
     * Postgres image with logical replication enabled. Mirrors what
     * `AtLeastOnceDeliveryIT` boots — same image, same `postgres -c`
     * args — so any future OrbStack tweak applied there carries over.
     */
    fun newPostgres(): PostgreSQLContainer<Nothing> {
        return PostgreSQLContainer<Nothing>(
            DockerImageName.parse("debezium/postgres:14-alpine").asCompatibleSubstituteFor("postgres"),
        ).apply {
            withDatabaseName("cdc")
            withUsername(PG_USER)
            withPassword(PG_PASSWORD)
            withCommand(
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=4",
                "-c",
                "max_wal_senders=4",
            )
        }
    }

    /** LocalStack with SNS + SQS enabled — same recipe as the existing IT. */
    fun newLocalStack(): LocalStackContainer {
        return LocalStackContainer(
            DockerImageName.parse(LOCALSTACK_IMAGE),
        ).withServices(SNS, SQS)
    }

    /**
     * MySQL container configured for ROW-format binary logging with full
     * row images and full row metadata. The replication user is created
     * at boot via Testcontainers; we just need to grant
     * `REPLICATION SLAVE` to it (done in the IT's `@BeforeAll` because
     * grant DDL runs after boot).
     *
     * Image pin: we target `mysql:8.0` (not `8.4`) because
     * `mysql-binlog-connector-java:0.29.2` — the version on this
     * project's classpath today — still issues
     * `SHOW MASTER STATUS` during the handshake. That statement was
     * removed in MySQL 8.4 (replaced by `SHOW BINARY LOG STATUS`).
     * Bumping the binlog client to 0.30.x lifts the constraint; that's
     * a follow-up build-wiring change tracked alongside Wave 5.1, and
     * intentionally out of scope here because this commit is forbidden
     * from touching the binlog-client coordinate in `build.gradle.kts`.
     *
     * Note: a server-id of 1 keeps the binlog client happy without
     * forcing the user to provision one explicitly.
     */
    fun newMysql(): MySQLContainer<Nothing> {
        return MySQLContainer<Nothing>(
            DockerImageName.parse(MYSQL_IMAGE),
        ).apply {
            withDatabaseName(MYSQL_DB)
            withUsername(MYSQL_USER)
            withPassword(MYSQL_PASSWORD)
            withCommand(
                "--server-id=1",
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--binlog-row-metadata=FULL",
            )
        }
    }

    /** Vanilla RabbitMQ with the management plugin — easier to debug. */
    fun newRabbitMq(): RabbitMQContainer {
        return RabbitMQContainer(DockerImageName.parse(RABBITMQ_IMAGE))
    }

    const val PG_USER = "postgres"
    const val PG_PASSWORD = "postgres"

    const val MYSQL_DB = "outbox"
    const val MYSQL_USER = "outbox_user"
    const val MYSQL_PASSWORD = "outbox_pwd"

    private const val LOCALSTACK_IMAGE = "localstack/localstack:3.8.1"
    private const val MYSQL_IMAGE = "mysql:8.0"
    private const val RABBITMQ_IMAGE = "rabbitmq:3.13-management"
}
