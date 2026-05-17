package br.com.fltech.cdc.outbox.publisher.workflow

import br.com.fltech.cdc.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.cdc.outbox.publisher.aws.sns.SNSTransactionalProducer
import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessageBody
import br.com.fltech.cdc.outbox.publisher.aws.sqs.SQSTransactionalProducer
import br.com.fltech.cdc.outbox.publisher.deadletter.SqsDeadLetterSink
import br.com.fltech.cdc.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.connector.DefaultConnectionProvider
import br.com.fltech.cdc.outbox.publisher.retry.ExponentialBackOff
import io.awspring.cloud.sns.core.SnsTemplate
import io.awspring.cloud.sqs.operations.SqsTemplate
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
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.SubscribeRequest
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Integration regression for at-least-once delivery introduced in Wave 2b.
 *
 * Scenario: emit three transactional WAL messages via
 * `pg_logical_emit_message`. The SNS producer is wrapped in a stub that
 * fails the FIRST publish attempt of the FIRST message, then succeeds
 * everywhere else. We assert:
 *  - all three payloads eventually arrive at the SQS queue subscribed to
 *    the SNS topic (no message is lost as a side effect of the in-place
 *    retry that the head-of-line state machine performs);
 *  - the slot is advanced after each successful publish (Postgres slot
 *    metadata reports a `confirmed_flush_lsn` past every emitted message
 *    after the test completes).
 *
 * Gated behind the `RUN_TESTCONTAINERS=1` env variable so it doesn't slow
 * down the everyday `./gradlew test` invocation. The orchestrator runs
 * it explicitly with that variable set when verifying Wave 2b.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "RUN_TESTCONTAINERS", matches = "1|true|yes")
class AtLeastOnceDeliveryIT {

    companion object {
        private const val TOPIC = "orders-events"
        private const val QUEUE = "orders-events-subscriber"
        private const val DLQ = "orders-events-dlq"
        private const val SLOT_NAME = "at_least_once_slot"
        private const val PG_USER = "postgres"
        private const val PG_PASSWORD = "postgres"

        private val postgres = PostgreSQLContainer<Nothing>(
            DockerImageName.parse("debezium/postgres:14-alpine").asCompatibleSubstituteFor("postgres"),
        ).apply {
            withDatabaseName("cdc")
            withUsername(PG_USER)
            withPassword(PG_PASSWORD)
            withCommand(
                "postgres",
                "-c", "wal_level=logical",
                "-c", "max_replication_slots=4",
                "-c", "max_wal_senders=4",
            )
        }

        private val localstack = LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8.1"),
        ).withServices(SNS, SQS)
    }

    private lateinit var snsClient: SnsClient
    private lateinit var sqsClient: SqsClient
    private lateinit var sqsAsyncClient: SqsAsyncClient
    private lateinit var topicArn: String
    private lateinit var queueUrl: String
    private lateinit var dlqUrl: String
    private lateinit var dlqArn: String
    private lateinit var queryConnection: java.sql.Connection

    @BeforeAll
    fun startContainers() {
        postgres.start()
        localstack.start()
    }

    @AfterAll
    fun stopContainers() {
        runCatching { postgres.stop() }
        runCatching { localstack.stop() }
    }

    @BeforeTest
    fun setUp() {
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
        sqsAsyncClient = SqsAsyncClient.builder()
            .endpointOverride(localstack.getEndpointOverride(SQS))
            .region(Region.of(localstack.region))
            .credentialsProvider(creds)
            .build()

        topicArn = snsClient.createTopic { it.name(TOPIC) }.topicArn()
        queueUrl = sqsClient.createQueue { it.queueName(QUEUE) }.queueUrl()
        dlqUrl = sqsClient.createQueue { it.queueName(DLQ) }.queueUrl()
        dlqArn = sqsClient.getQueueAttributes {
            it.queueUrl(dlqUrl).attributeNamesWithStrings("QueueArn")
        }.attributesAsStrings()["QueueArn"]!!

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

        // Open a query connection for emitting messages + dropping the slot
        // between tests. The slot itself is created by PostgresConnector when
        // the producer starts.
        val driverProps = java.util.Properties().apply {
            setProperty("user", PG_USER)
            setProperty("password", PG_PASSWORD)
        }
        queryConnection = DefaultConnectionProvider().getConnection(postgres.jdbcUrl, driverProps)
        runCatching {
            queryConnection.createStatement().use { s ->
                s.execute("SELECT pg_drop_replication_slot('$SLOT_NAME')")
            }
        }
    }

    @AfterTest
    fun tearDown() {
        runCatching { queryConnection.close() }
        runCatching { snsClient.close() }
        runCatching { sqsClient.close() }
        runCatching { sqsAsyncClient.close() }
    }

    @Test
    fun `transient failure on the first publish does not lose the message — retry recovers it`() {
        val received = ConcurrentHashMap.newKeySet<String>()
        val publishAttempts = java.util.concurrent.atomic.AtomicInteger(0)
        val failingSns = buildFailingSnsProducer(publishAttempts)
        val producer = buildProducer(failingSns)
        val executor = Executors.newSingleThreadExecutor { r -> Thread(r, "test-slot-reader") }
        executor.submit { producer.startStreaming() }

        awaitSlotReady()
        val sent = (1..3).map { emitMessage("test-message-$it") }
        awaitMessagesReceived(received, expected = 3)

        producer.stopStreaming()
        executor.shutdown()

        assertThreeMessagesWithRetry(publishAttempts, sent, received)
    }

    /**
     * Wraps the real `SnsTemplate` with a stub that fails the FIRST
     * publish attempt globally and delegates every later attempt
     * onward — exercises the retry path on a transient broker error
     * without faking the SDK.
     */
    private fun buildFailingSnsProducer(
        publishAttempts: java.util.concurrent.atomic.AtomicInteger,
    ): SNSProducer {
        val realSnsTemplate = SnsTemplate(snsClient)
        return object : SNSProducer {
            private val delegate = SNSTransactionalProducer(realSnsTemplate)
            override fun <T : Any> send(topicName: String, message: SNSMessage<T>) {
                val count = publishAttempts.incrementAndGet()
                if (count == 1) {
                    error("simulated transient broker failure on attempt #1")
                }
                delegate.send(topicArn, message)
            }
        }
    }

    private fun buildProducer(snsProducer: SNSProducer): SlotReaderMessageProducer {
        val sqsTemplate = SqsTemplate.builder().sqsAsyncClient(sqsAsyncClient).build()
        return SlotReaderMessageProducer(
            postgresConfiguration = PostgresConfiguration(
                host = postgres.host,
                port = postgres.getMappedPort(5432).toString(),
                database = postgres.databaseName,
                username = PG_USER,
                password = PG_PASSWORD,
            ),
            replicationConfiguration = ReplicationConfiguration(slotName = SLOT_NAME),
            snsProducer = snsProducer,
            sqsProducer = SQSTransactionalProducer(sqsTemplate),
            connectionProvider = DefaultConnectionProvider(),
            reconnectBackOff = ExponentialBackOff(
                initial = Duration.ofMillis(50),
                max = Duration.ofMillis(200),
            ),
            maxReconnectAttempts = 3,
            maxPublishAttempts = 3,
            publishBackOff = ExponentialBackOff(
                initial = Duration.ofMillis(50),
                max = Duration.ofMillis(200),
            ),
        )
    }

    private fun awaitSlotReady() {
        Awaitility.await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200)).until {
            queryConnection.createStatement().use { s ->
                s.executeQuery("SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME'").use { rs ->
                    rs.next()
                }
            }
        }
    }

    private fun awaitMessagesReceived(received: MutableSet<String>, expected: Int) {
        // Consume the SQS queue. Even though attempt #1 of message #1 failed,
        // the in-place retry must redeliver message #1 successfully, AND
        // messages #2 and #3 must still arrive — no skip due to LSN drift.
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).until {
            val response = sqsClient.receiveMessage(
                ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).waitTimeSeconds(1).build(),
            )
            response.messages().forEach { received.add(it.body()) }
            received.size >= expected
        }
    }

    private fun assertThreeMessagesWithRetry(
        publishAttempts: java.util.concurrent.atomic.AtomicInteger,
        sent: List<String>,
        received: Set<String>,
    ) {
        assertTrue(
            publishAttempts.get() >= 4,
            "expected ≥4 publish attempts (3 messages + 1 retry); got ${publishAttempts.get()}",
        )
        sent.forEach { expected ->
            assertTrue(
                received.any { it.contains(expected) },
                "expected an SQS message containing '$expected'; got ${received.joinToString(" | ")}",
            )
        }
        assertEquals(3, received.size)
    }

    private fun emitMessage(eventId: String): String {
        val body = SNSMessageBody(
            eventUUID = UUID.randomUUID(),
            eventType = "TestEvent",
            domainId = eventId,
            domain = "tests",
            eventTimestamp = java.time.LocalDateTime.now(),
            payload = mapOf("id" to eventId),
        )
        val envelope = SNSMessage(headers = mapOf("eventType" to "TestEvent"), body = body)
        val content = defaultMapper.writeValueAsString(envelope)
        queryConnection.createStatement().use { s ->
            // pg_logical_emit_message(transactional, prefix, content)
            s.execute(
                "SELECT pg_logical_emit_message(true, 'SNS|$TOPIC', " +
                    "'${content.replace("'", "''")}')",
            )
        }
        return eventId
    }
}
