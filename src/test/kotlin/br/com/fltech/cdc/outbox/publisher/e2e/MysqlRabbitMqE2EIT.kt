package br.com.fltech.cdc.outbox.publisher.e2e

import br.com.fltech.cdc.outbox.publisher.adapter.sink.rabbitmq.RabbitMqEventSink
import br.com.fltech.cdc.outbox.publisher.adapter.sink.registry.DefaultEventSinkRegistry
import br.com.fltech.cdc.outbox.publisher.adapter.source.mysql.MySqlBinlogRowChangeSource
import br.com.fltech.cdc.outbox.publisher.core.application.CdcProcessor
import br.com.fltech.cdc.outbox.publisher.core.application.DefaultMappingRules
import br.com.fltech.cdc.outbox.publisher.core.application.MappingCdcSource
import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange
import br.com.fltech.cdc.outbox.publisher.core.domain.TableMapping
import br.com.fltech.cdc.outbox.publisher.core.port.EventSink
import br.com.fltech.cdc.outbox.publisher.e2e.support.E2EContainers
import br.com.fltech.cdc.outbox.publisher.retry.ExponentialBackOff
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.GetResponse
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.RabbitMQContainer
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * End-to-end coverage for the **hexagonal** MySQL-to-RabbitMQ path.
 *
 * Hex chain wired by hand:
 *
 *  MySQL binlog (ROW format, FULL metadata, FULL image) ───▶
 *  `MySqlBinlogRowChangeSource` ───▶ `MappingCdcSource(rowSource, DefaultMappingRules(...))` ───▶
 *  `CdcProcessor` ───▶ `EventSinkRegistry { amqp → RabbitMqEventSink(RabbitTemplate) }` ───▶
 *  Rabbit exchange `outbox-x` ─binding(rk=orders.created)─▶ queue `outbox-q`
 *
 * The test creates the `outbox_events` table with a fixed column shape,
 * inserts one row, and asserts that:
 *
 *  - exactly one message arrives on the Rabbit queue,
 *  - its body matches the projected payload (renamed columns),
 *  - the message-id reflects the mapping key (derived from the row's
 *    `aggregate_id`).
 *
 * Column-name resolution from the binlog stream is a follow-up: the
 * Senior Dev agent is wiring it on `feat/wave-5.1` (information_schema
 * lookup driven by table-map events). Until that lands the binlog
 * adapter exposes columns by index — `col0`, `col1`, … — so this test
 * uses `TableMapping.payload.rename` to translate the indexed names
 * into domain names. The mapping is verbose by necessity; when Wave 5.1
 * lands the rename block collapses to a no-op and this test still passes
 * unchanged (the assertion is on the *outbound* payload shape, not on
 * the intermediate row shape).
 *
 * Gated on `RUN_TESTCONTAINERS=1|true|yes`.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "RUN_TESTCONTAINERS", matches = "1|true|yes")
class MysqlRabbitMqE2EIT {

    private val mysql: MySQLContainer<Nothing> = E2EContainers.newMysql()
    private val rabbit: RabbitMQContainer = E2EContainers.newRabbitMq()

    private lateinit var rabbitTemplate: RabbitTemplate
    private lateinit var rabbitConnectionFactory: CachingConnectionFactory
    private lateinit var jdbc: Connection

    @BeforeAll
    fun startContainers() {
        mysql.start()
        rabbit.start()

        // Grant REPLICATION SLAVE / CLIENT to the test user — required
        // by the binlog client to subscribe to the binary log stream.
        // Testcontainers' MySQL module already creates the user; we
        // only need the additional privileges here.
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
                    event_type   VARCHAR(64)  NOT NULL,
                    payload      VARCHAR(255) NOT NULL,
                    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """.trimIndent(),
            )
        }

        // Pre-create exchange + queue + binding via the AMQP client so
        // the test asserts on a real, named queue rather than relying
        // on the Rabbit sink to declare anything.
        val cf = ConnectionFactory().apply {
            host = rabbit.host
            port = rabbit.amqpPort
            username = rabbit.adminUsername
            password = rabbit.adminPassword
        }
        cf.newConnection().use { conn ->
            conn.createChannel().use { ch ->
                ch.exchangeDeclare(EXCHANGE, "topic", true)
                ch.queueDeclare(QUEUE, true, false, false, emptyMap())
                ch.queueBind(QUEUE, EXCHANGE, ROUTING_KEY)
                ch.queuePurge(QUEUE)
            }
        }

        // Spring AMQP RabbitTemplate against the Testcontainer.
        rabbitConnectionFactory = CachingConnectionFactory(rabbit.host, rabbit.amqpPort).apply {
            username = rabbit.adminUsername
            setPassword(rabbit.adminPassword)
        }
        rabbitTemplate = RabbitTemplate(rabbitConnectionFactory)
    }

    @AfterAll
    fun stopContainers() {
        runCatching {
            jdbc.createStatement().use { s -> s.execute("DROP TABLE IF EXISTS $OUTBOX_TABLE") }
        }
        runCatching { jdbc.close() }
        runCatching { rabbitConnectionFactory.destroy() }
        runCatching { rabbit.stop() }
        runCatching { mysql.stop() }
    }

    @Test
    fun `hexagonal chain delivers MySQL binlog inserts to RabbitMQ subscriber`() {
        val processor = buildProcessor()
        val executor = Executors.newSingleThreadExecutor { r -> Thread(r, "e2e-mysql-rabbit-processor") }
        try {
            executor.submit { processor.start() }
            waitForBinlogReady(processor)
            insertOrderRow()
            val response = waitForRabbitMessage()
            assertOrderPayload(response)
        } finally {
            processor.stop()
            executor.shutdownNow()
        }
    }

    private fun buildProcessor(): CdcProcessor {
        val rowSource = MySqlBinlogRowChangeSource(
            host = mysql.host,
            port = mysql.getMappedPort(MYSQL_PORT),
            username = mysql.username,
            password = mysql.password,
        )
        val source = MappingCdcSource(rowSource, DefaultMappingRules(listOf(buildMapping()), JSON_SERIALIZER))
        val sinks: Map<String, EventSink> = mapOf("amqp" to RabbitMqEventSink(rabbitTemplate))
        return CdcProcessor(
            source = source,
            sinkRegistry = DefaultEventSinkRegistry(sinks),
            maxPublishAttempts = MAX_PUBLISH_ATTEMPTS,
            publishBackOff = ExponentialBackOff(
                initial = Duration.ofMillis(BACKOFF_INITIAL_MS),
                max = Duration.ofMillis(BACKOFF_MAX_MS),
            ),
            slotLabel = "mysql-rabbit-e2e",
        )
    }

    /**
     * The binlog adapter currently surfaces columns by index. Until
     * Wave 5.1 (Senior Dev's parallel branch `feat/wave-5.1`) wires the
     * information_schema → column-name lookup, the mapping addresses
     * columns by their indexed name (`col0`..`col4`). Column order in
     * this table: col0=id, col1=aggregate_id, col2=event_type,
     * col3=payload, col4=created_at.
     *
     * Note: `DefaultMappingRules` derives the key from the RAW
     * (pre-rename) snapshot, then applies the rename when projecting
     * the payload. That ordering is why the key.format references
     * `{col1}` rather than the renamed `{aggregateId}`. Once Wave 5.1
     * lands, both blocks collapse to the domain names.
     */
    private fun buildMapping(): TableMapping = TableMapping(
        table = "${mysql.databaseName}.$OUTBOX_TABLE",
        capture = setOf(RowChange.Op.INSERT),
        key = TableMapping.Key(columns = listOf("col1"), format = "outbox:{col1}"),
        payload = TableMapping.Payload(
            exclude = listOf("col0", "col4"), // drop surrogate PK + created_at
            rename = mapOf("col1" to "aggregateId", "col2" to "eventType", "col3" to "payload"),
        ),
        eventType = TableMapping.EventType(template = "outbox.{op}"),
        routing = TableMapping.Routing(sink = "amqp://$EXCHANGE/$ROUTING_KEY", attributes = emptyMap()),
    )

    private fun waitForBinlogReady(processor: CdcProcessor) {
        // Wait for the orchestrator loop to flip running=true before we
        // hand it work. A brief grace then gives the binlog client time
        // to finish its handshake — an insert that races with the
        // handshake won't make it into the stream we subscribed to.
        Awaitility.await()
            .atMost(STARTUP_WAIT)
            .pollInterval(Duration.ofMillis(POLL_INTERVAL_MS))
            .until { processor.isRunning() }
        try {
            Thread.sleep(BINLOG_HANDSHAKE_GRACE_MS)
        } catch (ie: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    private fun insertOrderRow() {
        jdbc.createStatement().use { s ->
            s.execute(
                "INSERT INTO $OUTBOX_TABLE (aggregate_id, event_type, payload) " +
                    "VALUES ('order-1', 'OrderCreated', '{\"order\":1}')",
            )
        }
    }

    private fun waitForRabbitMessage(): GetResponse {
        val arrived = AtomicReference<GetResponse?>(null)
        Awaitility.await()
            .atMost(ASSERT_WAIT)
            .pollInterval(Duration.ofMillis(POLL_INTERVAL_MS))
            .until {
                val r = rabbitTemplate.execute { channel -> channel.basicGet(QUEUE, true) }
                if (r != null) arrived.set(r)
                arrived.get() != null
            }
        return assertNotNull(arrived.get(), "no message arrived on $QUEUE")
    }

    private fun assertOrderPayload(response: GetResponse) {
        val body = String(response.body, Charsets.UTF_8)
        assertTrue(body.contains("\"aggregateId\":\"order-1\""), "payload missing aggregateId: $body")
        assertTrue(body.contains("\"eventType\":\"OrderCreated\""), "payload missing eventType: $body")
        assertTrue(body.contains("\"payload\":\"{\\\"order\\\":1}\""), "payload missing inner payload: $body")
        assertEquals("outbox:order-1", response.props.messageId, "messageId mismatch")
    }

    companion object {
        private const val OUTBOX_TABLE = "outbox_events"
        private const val EXCHANGE = "outbox-x"
        private const val QUEUE = "outbox-q"
        private const val ROUTING_KEY = "orders.created"
        private const val MYSQL_PORT = 3306
        private const val BACKOFF_INITIAL_MS = 50L
        private const val BACKOFF_MAX_MS = 200L
        private const val POLL_INTERVAL_MS = 250L
        private const val BINLOG_HANDSHAKE_GRACE_MS = 1_500L
        private const val MAX_PUBLISH_ATTEMPTS = 3
        private val STARTUP_WAIT: Duration = Duration.ofSeconds(15)
        private val ASSERT_WAIT: Duration = Duration.ofSeconds(30)

        /**
         * Minimal JSON serialiser for the test payload — quotes strings,
         * leaves nothing else (the test row has only strings). The
         * production path plugs Jackson; the test stays self-contained.
         */
        private val JSON_SERIALIZER: (Map<String, Any?>) -> ByteArray = { map ->
            val entries = map.entries.joinToString(",") { (k, v) ->
                "\"$k\":${jsonValue(v)}"
            }
            "{$entries}".toByteArray(Charsets.UTF_8)
        }

        private fun jsonValue(v: Any?): String = when (v) {
            null -> "null"
            is Number, is Boolean -> v.toString()
            else -> "\"${v.toString().replace("\\", "\\\\").replace("\"", "\\\"")}\""
        }
    }
}
