package br.com.fltech.cdc.outbox.publisher.adapter.dlq.replay

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.domain.Routing
import br.com.fltech.cdc.outbox.publisher.core.port.EventSink
import br.com.fltech.cdc.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.cdc.outbox.publisher.observability.CdcOutboxMetrics
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End-to-end coverage of the DLQ replay chain against a real
 * SQS-compatible backend (LocalStack). Verifies that a message
 * written in the exact envelope shape produced by `SqsDeadLetterSink`
 * is readable by `SqsDlqReader`, parseable by `DlqEnvelope`, and
 * round-trips through `DlqReplayService` into a stub `EventSink`
 * that captures the re-published event. The same chain also
 * deletes the SQS message after publish.
 *
 * Gated on `RUN_TESTCONTAINERS=1|true|yes` so the default sweep
 * stays fast.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "RUN_TESTCONTAINERS", matches = "1|true|yes")
class DlqReplayIT {

    private val localstack: LocalStackContainer =
        LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8.1"))
            .withServices(LocalStackContainer.Service.SQS)

    private lateinit var sqs: SqsClient
    private lateinit var queueName: String
    private lateinit var queueUrl: String

    @BeforeAll
    fun startContainer() {
        localstack.start()
        sqs = SqsClient.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(localstack.accessKey, localstack.secretKey),
                ),
            )
            .region(Region.of(localstack.region))
            .build()
        queueName = "cdc-outbox-dlq-it"
        queueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(queueName).build()).queueUrl()
    }

    @AfterAll
    fun stopContainer() {
        runCatching { sqs.close() }
        runCatching { localstack.stop() }
    }

    @Test
    fun `replay reads SqsDeadLetterSink envelope shape and re-publishes into the registry`() {
        val payload = """{"orderId":42,"event":"OrderCreated"}"""
        val envelopeJson = jacksonObjectMapper().writeValueAsString(
            mapOf(
                "originalPrefix" to "sns://orders-events",
                "lsn" to "0/16E8198",
                "content" to payload,
                "failureType" to "TimeoutException",
                "failureMessage" to "publish timed out",
                "deadLetteredAt" to "2026-05-15T10:00:00Z",
            ),
        )
        sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(envelopeJson).build())

        val capturedEvents = CopyOnWriteArrayList<OutboxEvent>()
        val capturingSink = object : EventSink {
            override fun publish(routing: Routing, event: OutboxEvent) {
                capturedEvents.add(event)
            }
        }
        val registry = object : EventSinkRegistry {
            override fun publish(routing: Routing, event: OutboxEvent) {
                capturingSink.publish(routing, event)
            }
            override fun knownSchemes(): Set<String> = setOf("sns")
            override fun resolve(scheme: String): EventSink? =
                if (scheme == "sns") capturingSink else null
        }
        val service = DlqReplayService(
            reader = SqsDlqReader(sqs, queueName),
            sinkRegistry = registry,
            metrics = CdcOutboxMetrics.noop(),
        )

        val peeked = service.peek(10)
        assertEquals(1, peeked.size)
        assertEquals("sns://orders-events", peeked[0].envelope.originalPrefix)
        assertEquals("0/16E8198", peeked[0].envelope.lsn)

        val outcome = service.replay(peeked[0].handle, peeked[0].envelope)
        assertTrue(
            outcome is DlqReplayService.ReplayOutcome.Success,
            "expected Success but was $outcome",
        )

        assertEquals(1, capturedEvents.size)
        assertEquals("0/16E8198", capturedEvents[0].id)
        assertEquals(payload, String(capturedEvents[0].payload, Charsets.UTF_8))

        // Subsequent peek must come back empty — the replay deleted the message.
        // Wait for SQS to acknowledge the delete (eventual consistency on LocalStack).
        var retries = 10
        while (service.peek(10).isNotEmpty() && retries-- > 0) {
            Thread.sleep(POLL_INTERVAL_MS)
        }
        assertEquals(emptyList(), service.peek(10))
    }

    companion object {
        private const val POLL_INTERVAL_MS = 200L
    }
}
