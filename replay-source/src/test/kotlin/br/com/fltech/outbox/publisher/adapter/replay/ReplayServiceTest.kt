package br.com.fltech.outbox.publisher.adapter.replay

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.domain.Routing
import br.com.fltech.outbox.publisher.core.domain.RowChange
import br.com.fltech.outbox.publisher.core.port.EventSink
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import br.com.fltech.outbox.publisher.core.port.MappingRules
import br.com.fltech.outbox.publisher.core.port.RowChangeSource
import br.com.fltech.outbox.publisher.core.port.SourceReplayer
import br.com.fltech.outbox.publisher.core.port.UnsupportedReplayException
import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.awaitility.Awaitility
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ReplayServiceTest {
    private val meterRegistry = SimpleMeterRegistry()
    private val metrics = CdcOutboxMetrics(meterRegistry)
    private val capturedEvents = ConcurrentLinkedQueue<OutboxEvent>()
    private val registry = stubRegistry { _, event -> capturedEvents.add(event) }

    @Test
    fun `start dispatches replay to the matching sourceKind and surfaces job`() {
        val replayer = stubReplayer("mysql-binlog") { fakeRowChanges(3) }
        val service = newService(replayer)

        val job = service.startReplay(req("mysql-binlog", "f:1", "f:100"))

        assertEquals("mysql-binlog", job.sourceKind)
        Awaitility.await().atMost(java.time.Duration.ofSeconds(WAIT_SECONDS))
            .until { service.getJob(job.jobId)?.status == ReplayStatus.SUCCEEDED }
        val finished = service.getJob(job.jobId)!!
        assertEquals(3, finished.eventsProcessed)
        assertEquals(3, finished.eventsPublished)
        assertEquals(3, capturedEvents.size)
    }

    @Test
    fun `unknown sourceKind throws UnsupportedReplayException`() {
        val service = newService(stubReplayer("mysql-binlog") { emptyList() })

        assertThrows<UnsupportedReplayException> {
            service.startReplay(req("oracle-redo", "f:1", "f:100"))
        }
    }

    @Test
    fun `second concurrent start throws ConcurrentReplayException`() {
        val replayer = blockingStubReplayer("mysql-binlog")
        val service = newService(replayer)

        service.startReplay(req("mysql-binlog", "f:1", "f:100"))
        assertThrows<ConcurrentReplayException> {
            service.startReplay(req("mysql-binlog", "f:200", "f:300"))
        }
        replayer.release()
    }

    @Test
    fun `dryRun increments would-be counter and does NOT publish via registry`() {
        val service = newService(stubReplayer("mysql-binlog") { fakeRowChanges(5) })

        val job = service.startReplay(req("mysql-binlog", "f:1", "f:100", dryRun = true))

        Awaitility.await().atMost(java.time.Duration.ofSeconds(WAIT_SECONDS))
            .until { service.getJob(job.jobId)?.status == ReplayStatus.SUCCEEDED }
        val finished = service.getJob(job.jobId)!!
        assertEquals(5, finished.eventsThatWouldBePublished)
        assertEquals(0, finished.eventsPublished)
        assertEquals(0, capturedEvents.size)
    }

    @Test
    fun `override re-routes the published event to a different sink`() {
        val replayer = stubReplayer("mysql-binlog") { fakeRowChanges(1) }
        val service = newService(replayer)

        val job =
            service.startReplay(
                req("mysql-binlog", "f:1", "f:100", override = ReplayRequest.RoutingOverride("kafka", "topic-x")),
            )

        Awaitility.await().atMost(java.time.Duration.ofSeconds(WAIT_SECONDS))
            .until { service.getJob(job.jobId)?.status == ReplayStatus.SUCCEEDED }
        assertEquals(1, capturedEvents.size)
        val event = capturedEvents.first()!!
        assertEquals("kafka", event.routing.scheme)
        assertEquals("topic-x", event.routing.target)
    }

    @Test
    fun `publish failure does not abort the job - subsequent events still go through`() {
        val replayer = stubReplayer("mysql-binlog") { fakeRowChanges(3) }
        val flakyRegistry =
            object : EventSinkRegistry {
                private var i = 0

                override fun publish(
                    routing: Routing,
                    event: OutboxEvent,
                ) {
                    i += 1
                    if (i == 2) error("broker glitch")
                    capturedEvents.add(event)
                }

                override fun knownSchemes(): Set<String> = setOf("sns")

                override fun resolve(scheme: String): EventSink? = null
            }
        val service =
            ReplayService(
                replayers = mapOf("mysql-binlog" to replayer),
                mappingRules = identityRules(),
                sinkRegistry = flakyRegistry,
                metrics = metrics,
            )

        val job = service.startReplay(req("mysql-binlog", "f:1", "f:100"))

        Awaitility.await().atMost(java.time.Duration.ofSeconds(WAIT_SECONDS))
            .until { service.getJob(job.jobId)?.status == ReplayStatus.SUCCEEDED }
        val finished = service.getJob(job.jobId)!!
        assertEquals(2, finished.eventsPublished)
        assertEquals(1, finished.eventsPublishFailed)
        assertEquals(3, finished.eventsProcessed)
    }

    @Test
    fun `events filtered out by mapping rules are counted but not published`() {
        val replayer = stubReplayer("mysql-binlog") { fakeRowChanges(4) }
        // Mapping returns null for op=INSERT, mapping for op=UPDATE.
        // Fake rows alternate ops: 0=INSERT (filtered), 1=UPDATE, 2=INSERT (filtered), 3=UPDATE.
        val rules =
            object : MappingRules {
                override fun map(rowChange: RowChange): OutboxEvent? {
                    return if (rowChange.op == RowChange.Op.INSERT) null else fakeEvent(rowChange)
                }
            }
        val service =
            ReplayService(
                replayers = mapOf("mysql-binlog" to replayer),
                mappingRules = rules,
                sinkRegistry = registry,
                metrics = metrics,
            )

        val job = service.startReplay(req("mysql-binlog", "f:1", "f:100"))

        Awaitility.await().atMost(java.time.Duration.ofSeconds(WAIT_SECONDS))
            .until { service.getJob(job.jobId)?.status == ReplayStatus.SUCCEEDED }
        val finished = service.getJob(job.jobId)!!
        assertEquals(2, finished.eventsFilteredOut)
        assertEquals(2, finished.eventsPublished)
    }

    @Test
    fun `failed source open marks job as FAILED with error metadata`() {
        val replayer =
            object : SourceReplayer {
                override val sourceKind: String = "mysql-binlog"

                override fun openBoundedSource(
                    fromPosition: String,
                    toPosition: String,
                ): RowChangeSource {
                    throw UnsupportedReplayException("synthetic open failure")
                }
            }
        val service = newService(replayer)

        val job = service.startReplay(req("mysql-binlog", "f:1", "f:100"))

        Awaitility.await().atMost(java.time.Duration.ofSeconds(WAIT_SECONDS))
            .until { service.getJob(job.jobId)?.status == ReplayStatus.FAILED }
        val finished = service.getJob(job.jobId)!!
        assertEquals("UnsupportedReplayException", finished.errorClass)
        assertTrue(finished.errorMessage!!.contains("synthetic open failure"))
    }

    @Test
    fun `getJob returns null for unknown jobId`() {
        val service = newService(stubReplayer("mysql-binlog") { emptyList() })
        assertNull(service.getJob("nope"))
    }

    private fun newService(vararg replayers: SourceReplayer) =
        ReplayService(
            replayers = replayers.associateBy { it.sourceKind },
            mappingRules = identityRules(),
            sinkRegistry = registry,
            metrics = metrics,
        )

    private fun req(
        kind: String,
        from: String,
        to: String,
        dryRun: Boolean = false,
        override: ReplayRequest.RoutingOverride? = null,
    ) = ReplayRequest(sourceKind = kind, fromPosition = from, toPosition = to, dryRun = dryRun, override = override)

    private fun stubReplayer(
        kind: String,
        rowChanges: () -> List<RowChange>,
    ): SourceReplayer =
        object : SourceReplayer {
            override val sourceKind: String = kind

            override fun openBoundedSource(
                fromPosition: String,
                toPosition: String,
            ): RowChangeSource = listSource(rowChanges())
        }

    private fun blockingStubReplayer(kind: String) =
        object : SourceReplayer {
            private val released = java.util.concurrent.atomic.AtomicBoolean(false)
            override val sourceKind: String = kind

            override fun openBoundedSource(
                fromPosition: String,
                toPosition: String,
            ): RowChangeSource =
                object : RowChangeSource {
                    override fun open() = Unit

                    override fun poll(): RowChange? {
                        while (!released.get()) Thread.sleep(POLL_WAIT_MS)
                        return null
                    }

                    override fun ack(rowChange: RowChange) = Unit

                    override fun close() = Unit
                }

            fun release() {
                released.set(true)
            }
        }

    private fun listSource(items: List<RowChange>): RowChangeSource =
        object : RowChangeSource {
            private val iterator = items.iterator()

            override fun open() = Unit

            override fun poll(): RowChange? = if (iterator.hasNext()) iterator.next() else null

            override fun ack(rowChange: RowChange) = Unit

            override fun close() = Unit
        }

    private fun fakeRowChanges(count: Int): List<RowChange> =
        (0 until count).map { i ->
            RowChange(
                op = if (i % 2 == 0) RowChange.Op.INSERT else RowChange.Op.UPDATE,
                table = "public.orders",
                sourceCheckpoint = "mysql-bin.000001:$i",
                occurredAt = Instant.ofEpochSecond(1_700_000_000L + i),
                after = mapOf("id" to i, "value" to "row-$i"),
            )
        }

    private fun fakeEvent(rowChange: RowChange): OutboxEvent =
        OutboxEvent(
            id = rowChange.sourceCheckpoint,
            routing = Routing(scheme = "sns", target = "orders-events", attributes = emptyMap()),
            payload = """{"id":${rowChange.after?.get("id")}}""".toByteArray(),
            occurredAt = rowChange.occurredAt,
            sourceCheckpoint = rowChange.sourceCheckpoint,
        )

    private fun identityRules(): MappingRules =
        object : MappingRules {
            override fun map(rowChange: RowChange): OutboxEvent? = fakeEvent(rowChange)
        }

    private fun stubRegistry(publishFn: (Routing, OutboxEvent) -> Unit): EventSinkRegistry =
        object : EventSinkRegistry {
            override fun publish(
                routing: Routing,
                event: OutboxEvent,
            ) {
                publishFn(routing, event)
            }

            override fun knownSchemes(): Set<String> = setOf("sns", "kafka")

            override fun resolve(scheme: String): EventSink? = null
        }

    companion object {
        private const val WAIT_SECONDS = 5L
        private const val POLL_WAIT_MS = 50L
    }
}
