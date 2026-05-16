package br.com.fltech.cdc.outbox.publisher.adapter.replay

import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.e2e.support.E2EContainers
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.testcontainers.containers.MySQLContainer
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End-to-end IT: real MySQL container with binlog ROW format,
 * `MySqlBinlogReplayer` opens an isolated binlog session and
 * drains a configured window. Asserts that the events emitted by
 * the bounded source match the rows that were INSERT'd between
 * fromPosition and toPosition.
 *
 * Gated on `RUN_TESTCONTAINERS=1|true|yes`.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "RUN_TESTCONTAINERS", matches = "1|true|yes")
class MySqlBinlogReplayerIT {

    private val mysql: MySQLContainer<Nothing> = E2EContainers.newMysql()
    private lateinit var jdbc: Connection
    private lateinit var binlogDataSource: HikariDataSource

    @BeforeAll
    fun startContainers() {
        mysql.start()

        DriverManager.getConnection(mysql.jdbcUrl, "root", mysql.password).use { rootConn ->
            rootConn.createStatement().use { s ->
                s.execute(
                    "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* " +
                        "TO '${E2EContainers.MYSQL_USER}'@'%'",
                )
                s.execute("FLUSH PRIVILEGES")
            }
        }

        jdbc = DriverManager.getConnection(mysql.jdbcUrl, mysql.username, mysql.password)
        jdbc.createStatement().use { s ->
            s.execute("DROP TABLE IF EXISTS $OUTBOX_TABLE")
            s.execute(
                """
                CREATE TABLE $OUTBOX_TABLE (
                    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
                    aggregate_id VARCHAR(64)  NOT NULL,
                    payload      VARCHAR(255) NOT NULL
                )
                """.trimIndent(),
            )
        }

        binlogDataSource = HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = mysql.jdbcUrl
                username = mysql.username
                password = mysql.password
                maximumPoolSize = 2
                poolName = "replay-it-info-schema"
            },
        )
    }

    @AfterAll
    fun stopContainers() {
        runCatching {
            jdbc.createStatement().use { s -> s.execute("DROP TABLE IF EXISTS $OUTBOX_TABLE") }
        }
        runCatching { jdbc.close() }
        runCatching { binlogDataSource.close() }
        runCatching { mysql.stop() }
    }

    @Test
    fun `replayer drains a bounded binlog window and emits each row as RowChange`() {
        val (startFile, startPosition) = currentBinlogPosition()

        insertOrders(rows = 5)

        val (stopFile, stopPosition) = currentBinlogPosition()

        val replayer = MySqlBinlogReplayer(
            host = mysql.host,
            port = mysql.getMappedPort(MYSQL_PORT),
            username = mysql.username,
            password = mysql.password,
            serverId = REPLAY_SERVER_ID,
            dataSource = binlogDataSource,
        )

        val source = replayer.openBoundedSource("$startFile:$startPosition", "$stopFile:$stopPosition")
        val captured = CopyOnWriteArrayList<RowChange>()
        source.open()
        try {
            Awaitility.await()
                .atMost(Duration.ofSeconds(WAIT_SECONDS))
                .pollInterval(Duration.ofMillis(POLL_INTERVAL_MS))
                .until {
                    val rc = source.poll() ?: return@until captured.size >= 5
                    captured.add(rc)
                    captured.size >= 5
                }
        } finally {
            source.close()
        }

        assertEquals(5, captured.size, "expected 5 inserts to be replayed; got ${captured.size}")
        captured.forEachIndexed { idx, rowChange ->
            assertEquals(RowChange.Op.INSERT, rowChange.op)
            assertEquals("${mysql.databaseName}.$OUTBOX_TABLE", rowChange.table)
            val after = rowChange.after
            assertTrue(after != null && after.containsKey("aggregate_id"))
            // `INFORMATION_SCHEMA` lookup was wired via HikariDataSource
            // — names should NOT have fallen back to col0/col1.
            assertTrue(
                after.containsKey("aggregate_id") && after.containsKey("payload"),
                "after-map should carry aggregate_id+payload; got $after",
            )
            assertEquals("order-$idx", after["aggregate_id"])
        }
    }

    private fun currentBinlogPosition(): Pair<String, Long> {
        jdbc.createStatement().use { s ->
            s.executeQuery("SHOW MASTER STATUS").use { rs ->
                check(rs.next()) { "SHOW MASTER STATUS returned no rows" }
                return rs.getString("File") to rs.getLong("Position")
            }
        }
    }

    private fun insertOrders(rows: Int) {
        jdbc.prepareStatement(
            "INSERT INTO $OUTBOX_TABLE (aggregate_id, payload) VALUES (?, ?)",
        ).use { stmt ->
            (0 until rows).forEach { i ->
                stmt.setString(1, "order-$i")
                stmt.setString(2, """{"orderId":$i}""")
                stmt.executeUpdate()
            }
        }
    }

    companion object {
        private const val OUTBOX_TABLE = "orders"
        private const val MYSQL_PORT = 3306
        private const val REPLAY_SERVER_ID = 1_048_576L
        private const val WAIT_SECONDS = 30L
        private const val POLL_INTERVAL_MS = 100L
    }
}
