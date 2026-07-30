package br.com.fltech.outbox.publisher.e2e

import br.com.fltech.outbox.publisher.adapter.sink.registry.DefaultEventSinkRegistry
import br.com.fltech.outbox.publisher.adapter.sink.sns.SnsEventSink
import br.com.fltech.outbox.publisher.adapter.source.postgres.PgLogicalReplicationCdcSource
import br.com.fltech.outbox.publisher.core.application.CdcProcessor
import br.com.fltech.outbox.publisher.core.port.EventSink
import br.com.fltech.outbox.publisher.e2e.support.E2EContainers
import br.com.fltech.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.outbox.publisher.replication.connector.DefaultConnectionProvider
import br.com.fltech.outbox.publisher.retry.ExponentialBackOff
import io.awspring.cloud.sns.core.SnsTemplate
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service.SNS
import org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.SubscribeRequest
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import java.sql.Connection
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.test.assertTrue

/**
 * End-to-end coverage for the **hexagonal** Postgres-to-SNS path.
 *
 * Wires the hex chain by hand — no Spring context — to keep the
 * blast radius of the test small:
 *
 *  Postgres (logical replication, wal2json) ───▶
 *  `PgLogicalReplicationCdcSource` ─▶ `CdcProcessor` ─▶
 *  `EventSinkRegistry { sns → SnsEventSink(SnsTemplate(LocalStack)) }`
 *  ───▶ LocalStack SNS topic ───▶ subscribed LocalStack SQS queue
 *
 * Three messages are emitted with `pg_logical_emit_message(true, 'sns://<topic>', payload)`.
 * The test asserts all three land on the SQS subscriber. The sink is
 * straight-through (no injected failure) — the point is to prove the
 * hex chain works end to end, NOT to re-test retry (that's
 * `AtLeastOnceDeliveryIT`'s job).
 *
 * Gated on `RUN_TESTCONTAINERS=1|true|yes` so the default
 * `./gradlew test` slice (unit-only, no Docker) stays fast.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "RUN_TESTCONTAINERS", matches = "1|true|yes")
class PostgresSnsE2EIT {

    private val postgres: PostgreSQLContainer<Nothing> = E2EContainers.newPostgres()
    private val localstack: LocalStackContainer = E2EContainers.newLocalStack()

    private lateinit var snsClient: SnsClient
    private lateinit var sqsClient: SqsClient
    private lateinit var topicArn: String
    private lateinit var queueUrl: String
    private lateinit var queryConnection: Connection

    @BeforeAll
    fun startContainers() {
        postgres.start()
        localstack.start()

        val creds = StaticCredentialsProvider.create(
            AwsBasicCredentials.create(localstack.accessKey, localstack.secretKey),
        )
        snsClient = SnsClient.builder()
            .endpointOverride(localstack.getEndpointOverride(SNS))
            .region(Region.of(localstack.region))
            .credentialsProvider(creds)
            .build()
        sqsClient = SqsClient.builder()
            .endpointOverride(localstack.getEndpointOverride(SQS))
            .region(Region.of(localstack.region))
            .credentialsProvider(creds)
            .build()

        topicArn = snsClient.createTopic { it.name(TOPIC) }.topicArn()
        queueUrl = sqsClient.createQueue { it.queueName(QUEUE) }.queueUrl()
        val queueArn = sqsClient.getQueueAttributes {
            it.queueUrl(queueUrl).attributeNamesWithStrings("QueueArn")
        }.attributesAsStrings()["QueueArn"]!!
        snsClient.subscribe(
            SubscribeRequest.builder()
                .topicArn(topicArn)
                .protocol("sqs")
                .endpoint(queueArn)
                .attributes(mapOf("RawMessageDelivery" to "true"))
                .build(),
        )

        // Connection for emitting messages + dropping any leftover slot.
        val driverProps = Properties().apply {
            setProperty("user", E2EContainers.PG_USER)
            setProperty("password", E2EContainers.PG_PASSWORD)
        }
        queryConnection = DefaultConnectionProvider().getConnection(postgres.jdbcUrl, driverProps)
        dropSlotIfPresent()
    }

    @AfterAll
    fun stopContainers() {
        runCatching { dropSlotIfPresent() }
        runCatching { queryConnection.close() }
        runCatching { snsClient.deleteTopic { it.topicArn(topicArn) } }
        runCatching { sqsClient.deleteQueue { it.queueUrl(queueUrl) } }
        runCatching { snsClient.close() }
        runCatching { sqsClient.close() }
        runCatching { postgres.stop() }
        runCatching { localstack.stop() }
    }

    @Test
    fun `hexagonal chain delivers Postgres WAL messages to SNS subscriber`() {
        val processor = buildProcessor()
        val executor = Executors.newSingleThreadExecutor { r -> Thread(r, "e2e-pg-sns-processor") }
        val received = ConcurrentHashMap.newKeySet<String>()
        try {
            executor.submit { processor.start() }
            awaitSlotExists()
            val expectedBodies = (1..3).map { "payload-$it" }
            expectedBodies.forEach { emitMessage(it) }
            drainQueueUntilAllPresent(expectedBodies, received)
            expectedBodies.forEach { expected ->
                assertTrue(
                    received.any { it == expected },
                    "expected an SQS body equal to '$expected'; got [${received.joinToString(" | ")}]",
                )
            }
        } finally {
            // Stop / clean up even if Awaitility timed out — otherwise the
            // processor thread keeps the slot busy and the next run
            // would collide with it.
            processor.stop()
            executor.shutdownNow()
        }
    }

    private fun buildProcessor(): CdcProcessor {
        val source = PgLogicalReplicationCdcSource(
            postgresConfiguration = PostgresConfiguration(
                host = postgres.host,
                port = postgres.getMappedPort(POSTGRES_PORT).toString(),
                database = postgres.databaseName,
                username = E2EContainers.PG_USER,
                password = E2EContainers.PG_PASSWORD,
            ),
            replicationConfiguration = ReplicationConfiguration(slotName = SLOT_NAME),
            connectionProvider = DefaultConnectionProvider(),
            objectMapper = defaultMapper,
        )
        val sinks: Map<String, EventSink> = mapOf("sns" to SnsEventSink(SnsTemplate(snsClient)))
        return CdcProcessor(
            source = source,
            sinkRegistry = DefaultEventSinkRegistry(sinks),
            maxPublishAttempts = MAX_PUBLISH_ATTEMPTS,
            publishBackOff = ExponentialBackOff(
                initial = Duration.ofMillis(BACKOFF_INITIAL_MS),
                max = Duration.ofMillis(BACKOFF_MAX_MS),
            ),
            slotLabel = "postgres-sns-e2e",
        )
    }

    /** The slot is created lazily inside the worker thread; wait for it. */
    private fun awaitSlotExists() = Awaitility.await()
        .atMost(STARTUP_WAIT)
        .pollInterval(Duration.ofMillis(POLL_INTERVAL_MS))
        .until { slotExists() }

    private fun drainQueueUntilAllPresent(expected: List<String>, into: MutableSet<String>) {
        Awaitility.await()
            .atMost(ASSERT_WAIT)
            .pollInterval(Duration.ofMillis(POLL_INTERVAL_MS))
            .until {
                val response = sqsClient.receiveMessage(
                    ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(MAX_MESSAGES_PER_POLL)
                        .waitTimeSeconds(1)
                        .build(),
                )
                response.messages().forEach { into.add(it.body()) }
                into.size >= expected.size
            }
    }

    private fun emitMessage(body: String) {
        queryConnection.createStatement().use { s ->
            // pg_logical_emit_message(transactional, prefix, content).
            // Prefix uses the URI form parsed by `Routing.parsePrefix`.
            s.execute(
                "SELECT pg_logical_emit_message(true, 'sns://$TOPIC', " +
                    "'${body.replace("'", "''")}')",
            )
        }
    }

    private fun slotExists(): Boolean = queryConnection.createStatement().use { s ->
        s.executeQuery("SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'").use { rs ->
            rs.next()
        }
    }

    private fun dropSlotIfPresent() {
        runCatching {
            queryConnection.createStatement().use { s ->
                s.execute("SELECT pg_drop_replication_slot('$SLOT_NAME')")
            }
        }
    }

    companion object {
        private const val TOPIC = "hex-orders-events"
        private const val QUEUE = "hex-orders-events-subscriber"
        private const val SLOT_NAME = "hex_pg_sns_e2e_slot"
        private const val POSTGRES_PORT = 5432
        private const val BACKOFF_INITIAL_MS = 50L
        private const val BACKOFF_MAX_MS = 200L
        private const val POLL_INTERVAL_MS = 250L
        private const val MAX_MESSAGES_PER_POLL = 10
        private const val MAX_PUBLISH_ATTEMPTS = 3
        private val STARTUP_WAIT: Duration = Duration.ofSeconds(15)
        private val ASSERT_WAIT: Duration = Duration.ofSeconds(30)
    }
}
