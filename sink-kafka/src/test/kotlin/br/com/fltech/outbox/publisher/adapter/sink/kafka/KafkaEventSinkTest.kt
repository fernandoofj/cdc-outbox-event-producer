package br.com.fltech.outbox.publisher.adapter.sink.kafka

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.time.Instant
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals

class KafkaEventSinkTest {

    @Test
    fun `publish writes a ProducerRecord with id as key, payload as value, headers as Kafka headers`() {
        val template = mockk<KafkaTemplate<String, ByteArray>>()
        val captured = slot<ProducerRecord<String, ByteArray>>()
        val future = CompletableFuture<SendResult<String, ByteArray>>().apply {
            // Resolve immediately so `.get()` returns instantly.
            complete(
                SendResult(
                    ProducerRecord("dummy", "dummy", ByteArray(0)),
                    RecordMetadata(TopicPartition("dummy", 0), 0, 0, 0L, 0, 0),
                ),
            )
        }
        every { template.send(capture(captured)) } returns future
        val sink = KafkaEventSink(template)

        val routing = Routing("kafka", "orders.events", mapOf("env" to "prod"))
        val event = OutboxEvent(
            id = "domain-1",
            routing = routing,
            payload = "hello".toByteArray(),
            occurredAt = Instant.EPOCH,
            sourceCheckpoint = "ck",
            headers = mapOf("trace-id" to "abc"),
        )

        sink.publish(routing, event)

        val record = captured.captured
        verify(exactly = 1) { template.send(any<ProducerRecord<String, ByteArray>>()) }
        assertEquals("orders.events", record.topic())
        assertEquals("domain-1", record.key())
        assertEquals("hello", String(record.value(), Charsets.UTF_8))
        val headers = record.headers().toList().associate { it.key() to String(it.value(), Charsets.UTF_8) }
        assertEquals(mapOf("trace-id" to "abc", "env" to "prod"), headers)
    }
}
