package br.com.fltech.cdc.outbox.publisher.workflow

import br.com.fltech.cdc.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.cdc.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.cdc.outbox.publisher.deadletter.DeadLetterSink
import br.com.fltech.cdc.outbox.publisher.helper.JsonHelper
import br.com.fltech.cdc.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.cdc.outbox.publisher.observability.CdcOutboxMetrics
import br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.connector.ConnectionProvider
import br.com.fltech.cdc.outbox.publisher.replication.connector.HikariCPConnectionProvider
import br.com.fltech.cdc.outbox.publisher.replication.connector.PostgresConnector
import br.com.fltech.cdc.outbox.publisher.replication.model.MessageChange
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserImplV1
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserImplV2
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserStrategy
import br.com.fltech.cdc.outbox.publisher.retry.BackOff
import br.com.fltech.cdc.outbox.publisher.retry.ExponentialBackOff
import org.postgresql.replication.LogSequenceNumber
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Long-running loop that consumes a Postgres logical-replication slot, parses
 * each WAL record, and publishes outbound events to the configured sinks.
 *
 * One instance is bound to one slot; concurrent instances on the same slot
 * are an error (Postgres enforces this with SQLSTATE 55006). The loop is
 * single-threaded.
 *
 * Wave 1 improvements over the original implementation:
 *  - `running` is `@Volatile` so `stopStreaming()` is observed promptly.
 *  - The LSN of the message being acted on is captured at read time and
 *    threaded through the callbacks. The previous code read
 *    `lastReceivedLsn()` at success/failure time, which drifts ahead of the
 *    in-flight message and could silently skip a failed-then-succeeded
 *    sequence.
 *  - Reconnects on any `SQLException` (not only recovery mode) honour an
 *    injectable [BackOff] policy with jitter, capped at [maxReconnectAttempts].
 *  - Optional Micrometer metrics via [CdcOutboxMetrics].
 *
 * Per-message LSN resolution (closed in Wave 1.5):
 *  - When the wal2json output plugin has `include-lsn=true` (the default,
 *    see [ReplicationConfiguration.includeLsn]), each emitted record carries
 *    its own LSN in the `lsn` JSON field. The producer prefers that exact
 *    value over the stream's high-water mark when advancing the slot,
 *    eliminating the residual race between `readPending()` and
 *    `lastReceiveLSN()`.
 *  - Fallback: if the `lsn` field is missing (older wal2json builds or
 *    `include-lsn=false`), the producer reverts to the captured
 *    `lastReceiveLSN` immediately after `readPending`. The drift caveat
 *    documented previously applies only on that fallback path.
 */
// TooManyFunctions: this class is the orchestrator. The functions are
// short, focused, and each plays a distinct role in the streaming loop.
// Will be split into application/CdcProcessor + adapter when the
// hexagonal refactor lands (Wave 3+).
@Suppress("TooManyFunctions", "LongParameterList")
class SlotReaderMessageProducer(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration,
    private val snsProducer: SNSProducer,
    private val sqsProducer: SQSProducer,
    private val connectionProvider: ConnectionProvider = HikariCPConnectionProvider(),
    private val metrics: CdcOutboxMetrics = CdcOutboxMetrics.noop(),
    private val reconnectBackOff: BackOff = ExponentialBackOff(),
    private val maxReconnectAttempts: Int = DEFAULT_MAX_RECONNECT_ATTEMPTS,
    /**
     * Optional dead-letter sink. When null, head-of-line retries
     * continue indefinitely until success, restart, or operator
     * intervention — the slot stays put on the failing LSN. When set,
     * messages that exhaust [maxPublishAttempts] are forwarded here and
     * the slot is allowed to advance past them.
     */
    private val deadLetterSink: DeadLetterSink? = null,
    /**
     * Maximum publish attempts per message, INCLUDING the initial
     * attempt. After this many failures, the message is dead-lettered
     * if [deadLetterSink] is set, or kept stuck otherwise.
     */
    private val maxPublishAttempts: Int = DEFAULT_MAX_PUBLISH_ATTEMPTS,
    /**
     * Back-off policy applied BETWEEN publish retries on the same
     * message (separate from [reconnectBackOff] which governs the
     * outer connection-level loop).
     */
    private val publishBackOff: BackOff = ExponentialBackOff(
        initial = Duration.ofMillis(DEFAULT_PUBLISH_BACKOFF_INITIAL_MS),
        max = Duration.ofSeconds(DEFAULT_PUBLISH_BACKOFF_MAX_SECONDS),
    ),
) {
    // `running` is `internal` so unit tests can drive the retry/dead-letter
    // state machine without going through the full streaming loop.
    @Volatile
    internal var running = false

    @Volatile
    private var lastFlushedTime: Long = 0
    private var reconnectAttempt: Int = 0

    /**
     * LSN of the last message whose publish failed and has NOT yet been
     * redelivered successfully. While this is non-null the idle-flush path
     * MUST NOT fast-forward the slot, otherwise Postgres would recycle WAL
     * past the failed message and break the at-least-once contract.
     *
     * Volatile because the Wave 2 health indicator reads this from a
     * different thread (`HealthIndicator.health()` is called by the
     * Boot Actuator HTTP worker pool, not the streaming thread).
     */
    @Volatile
    private var pendingFailureLsn: LogSequenceNumber? = null

    private lateinit var slotReaderCallback: SlotReaderCallback
    private val byteToClassParserImplV1 = ByteToClassParserImplV1(defaultMapper)
    private val byteToClassParserImplV2 = ByteToClassParserImplV2(defaultMapper)
    private val byteToClassParser = ByteToClassParserStrategy(byteToClassParserImplV1, byteToClassParserImplV2)
        .selectParser(replicationConfiguration)

    fun startStreaming() {
        // Flip to true here (not at field-init time) so that
        // `snapshotState().running` reports false between construction and
        // the first iteration of the loop, which matters for the Spring
        // Boot health indicator when Actuator probes the context before
        // the lifecycle has finished its start phase.
        running = true
        while (running) {
            readingSlotData()
        }
    }

    fun stopStreaming() {
        running = false
    }

    fun resetIdleCounter() {
        lastFlushedTime = System.currentTimeMillis()
    }

    /**
     * Read-only snapshot of producer state for health checks. Safe to call
     * from a different thread (relies on `@Volatile` fields).
     */
    fun snapshotState(): ProducerState = ProducerState(
        slot = replicationConfiguration.slotName,
        running = running,
        pendingFailureLsn = pendingFailureLsn?.toString(),
        msSinceLastActivity = msSinceLastFlush(),
    )

    private fun msSinceLastFlush(): Long {
        val ts = lastFlushedTime
        return if (ts == 0L) Long.MAX_VALUE else System.currentTimeMillis() - ts
    }

    data class ProducerState(
        val slot: String,
        val running: Boolean,
        val pendingFailureLsn: String?,
        /**
         * Milliseconds since the streaming loop last observed activity
         * (`resetIdleCounter`). `Long.MAX_VALUE` means the loop has never
         * started or has not yet processed its first iteration.
         */
        val msSinceLastActivity: Long,
    )

    // TooGenericExceptionCaught: the streaming loop intentionally treats
    // every unhandled exception as recoverable — anything that escapes
    // here would otherwise kill the worker thread silently, which is
    // worse than reconnecting. Each branch tags the reason for metrics.
    @Suppress("TooGenericExceptionCaught")
    private fun readingSlotData() {
        try {
            createPostgresConnector(postgresConfiguration, replicationConfiguration).use { postgresConnector ->
                initializeCallback(postgresConnector)
                resetIdleCounter()
                reconnectAttempt = 0
                logger.info("Consuming from slot {}", replicationConfiguration.slotName)
                while (running) {
                    readDataPostingToDestination(postgresConnector)
                }
            }
        } catch (sqlException: SQLException) {
            handleReconnectableError(REASON_SQL, sqlException, sqlException.sqlState == RECOVERY_MODE_SQL_STATE)
        } catch (ioException: IOException) {
            handleReconnectableError(REASON_IO, ioException, force = false)
        } catch (exception: Exception) {
            logger.error("Received unexpected exception of type {}", exception.javaClass, exception)
            metrics.recordReconnect(REASON_UNKNOWN)
            sleepBeforeReconnect()
        }
    }

    private fun handleReconnectableError(reason: String, cause: Throwable, force: Boolean) {
        logger.error("Replication stream error ({}), will reconnect", reason, cause)
        metrics.recordReconnect(reason)
        if (force) {
            logger.info("Postgres reports recovery mode; sleeping {} ms before reconnect", RECOVERY_MODE_SLEEP_MILLIS)
            sleepInterruptibly(RECOVERY_MODE_SLEEP_MILLIS)
        } else {
            sleepBeforeReconnect()
        }
    }

    private fun sleepBeforeReconnect() {
        reconnectAttempt += 1
        if (reconnectAttempt > maxReconnectAttempts) {
            logger.error(
                "Exhausted {} reconnect attempts; stopping cooperatively. " +
                    "An operator must restart the producer (the slot will resume from its last flushed LSN).",
                maxReconnectAttempts,
            )
            running = false
            return
        }
        val delay = reconnectBackOff.nextDelay(reconnectAttempt)
        logger.info(
            "Backing off {} ms before reconnect attempt #{}/{}",
            delay.toMillis(),
            reconnectAttempt,
            maxReconnectAttempts,
        )
        sleepInterruptibly(delay.toMillis())
    }

    private fun sleepInterruptibly(millis: Long) {
        if (millis <= 0L || !running) return
        try {
            Thread.sleep(millis)
        } catch (interruptedException: InterruptedException) {
            logger.warn("Interrupted while sleeping; stopping cooperatively", interruptedException)
            Thread.currentThread().interrupt()
            running = false
        }
    }

    private fun createPostgresConnector(
        postgresConfiguration: PostgresConfiguration,
        replicationConfiguration: ReplicationConfiguration,
    ): PostgresConnector =
        PostgresConnector(postgresConfiguration, replicationConfiguration, connectionProvider)

    // `internal` so tests can wire a callback with a mocked PostgresConnector
    // and exercise [processMessage] in isolation.
    internal fun initializeCallback(postgresConnector: PostgresConnector) {
        slotReaderCallback = SlotReaderCallback(this, postgresConnector, metrics)
    }

    private fun readDataPostingToDestination(postgresConnector: PostgresConnector) {
        val msg = postgresConnector.readPending()

        if (msg != null) {
            // Capture the LSN of THIS message immediately. `lastReceivedLsn()` is the
            // high-water mark of the stream and drifts ahead as more bytes arrive while
            // we are publishing; using it at success/failure time silently skips messages.
            // (See class-level KDoc about the Wave 1.5 follow-up that closes the residual
            // gap by parsing the LSN from the wal2json `nextlsn` field.)
            val capturedLsn = postgresConnector.lastReceivedLsn()
            metrics.recordMessageRead(replicationConfiguration.slotName)
            processData(msg, capturedLsn)
            return
        }

        if (pendingFailureLsn != null) {
            // Do NOT fast-forward: there is an unacknowledged failure ahead of the slot's
            // confirmed LSN. Advancing here would let Postgres recycle WAL past that
            // message and break at-least-once. Stay idle until the next read or until
            // the loop is restarted; the publisher will retry on the next non-empty read.
            return
        }

        val currentTimeMillis = System.currentTimeMillis()
        val updateIdleSlotIntervalMillis =
            TimeUnit.SECONDS.toMillis(replicationConfiguration.updateIdleSlotInterval)
        if (currentTimeMillis - lastFlushedTime > updateIdleSlotIntervalMillis) {
            val lsn = postgresConnector.currentLSN()
            logger.info(
                "Fast-forwarding stream LSN to {} due to inactivity on slot {}",
                lsn,
                replicationConfiguration.slotName,
            )
            postgresConnector.setStreamLsn(lsn)
            resetIdleCounter()
        }
    }

    private fun processData(byteBufferMessage: ByteBuffer, lsn: LogSequenceNumber) {
        val changes = byteToClassParser.parse(byteBufferMessage)
        val nonEmpty = changes.takeIf { it.isNotEmpty() }
        if (nonEmpty == null) {
            slotReaderCallback.discardMessage(lsn, KIND_EMPTY)
            return
        }
        nonEmpty.forEach { change ->
            when (change.kind) {
                KIND_MESSAGE -> processMessage(change as MessageChange, lsn)
                else -> slotReaderCallback.discardMessage(lsn, change.kind)
            }
        }
    }

    // TooGenericExceptionCaught: catching the full Exception hierarchy is
    // intentional here so that an unexpected sink-specific exception does
    // not crash the streaming loop. The captured throwable drives the
    // retry/dead-letter state machine; the slot only advances on success
    // (per-message LSN), explicit discard, or successful dead-letter.
    // `internal` so unit tests can drive it without spinning the full loop.
    @Suppress("TooGenericExceptionCaught")
    internal fun processMessage(messageChange: MessageChange, fallbackLsn: LogSequenceNumber) {
        // Prefer the per-message LSN carried inside the wal2json payload over
        // the stream's high-water-mark fallback (see class KDoc).
        val lsn = resolveLsn(messageChange, fallbackLsn)
        val prefixPair = parsePrefix(messageChange.prefix)
        val destinationName = prefixPair.second
        val sink = prefixPair.first.name.lowercase()
        logger.info("Processing message for prefix {} (lsn {})", messageChange.prefix, lsn)

        var attempt = 0
        var lastException: Exception? = null
        while (attempt < maxPublishAttempts && running) {
            attempt += 1
            try {
                when (prefixPair.first) {
                    DestinationType.SNS -> processSNSMessage(destinationName, messageChange, lsn)
                    DestinationType.SQS -> processSQSMessage(destinationName, messageChange, lsn)
                }
                // Successful publish: clear the pending failure marker so the
                // idle-flush path can resume advancing the slot on quiet stretches.
                if (pendingFailureLsn == lsn) {
                    pendingFailureLsn = null
                }
                return
            } catch (e: Exception) {
                lastException = e
                pendingFailureLsn = lsn
                slotReaderCallback.onFailure(lsn, messageChange.prefix, sink, e)
                if (attempt < maxPublishAttempts) {
                    metrics.recordRetry(sink = sink, topic = messageChange.prefix, attempt = attempt)
                    val delay = publishBackOff.nextDelay(attempt)
                    logger.warn(
                        "Publish attempt #{}/{} for prefix {} (lsn {}) failed: {}. Sleeping {} ms before retry.",
                        attempt,
                        maxPublishAttempts,
                        messageChange.prefix,
                        lsn,
                        e.javaClass.simpleName,
                        delay.toMillis(),
                    )
                    sleepInterruptibly(delay.toMillis())
                }
            }
        }

        // Exhausted retries (or stopped). Try dead-letter; if it succeeds, advance
        // the slot. If not, stay stuck — operator must intervene.
        val cause = lastException ?: IllegalStateException("publish retries exhausted without an exception")
        handleExhaustedRetries(lsn, messageChange, sink, cause)
    }

    private fun handleExhaustedRetries(
        lsn: LogSequenceNumber,
        messageChange: MessageChange,
        sink: String,
        cause: Throwable,
    ) {
        val dlq = deadLetterSink
        if (dlq == null) {
            logger.error(
                "Publish exhausted {} attempts for prefix {} (lsn {}) and no dead-letter sink is configured — " +
                    "the slot will stay at the last successful LSN. Operator intervention required.",
                maxPublishAttempts,
                messageChange.prefix,
                lsn,
                cause,
            )
            return
        }
        try {
            dlq.send(lsn, messageChange, cause)
            metrics.recordDeadLettered(
                sink = sink,
                topic = messageChange.prefix,
                cause = cause.javaClass.simpleName,
            )
            slotReaderCallback.discardMessage(lsn, type = REASON_DEAD_LETTERED)
            logger.warn(
                "Dead-lettered message for prefix {} (lsn {}) after {} failed attempts ({})",
                messageChange.prefix,
                lsn,
                maxPublishAttempts,
                cause.javaClass.simpleName,
            )
        } catch (dlqException: Exception) {
            metrics.recordDeadLetterFailure(cause = dlqException.javaClass.simpleName)
            logger.error(
                "Dead-letter publish failed for prefix {} (lsn {}); slot stays at last successful LSN. " +
                    "Original cause: {}; DLQ cause: {}",
                messageChange.prefix,
                lsn,
                cause.javaClass.simpleName,
                dlqException.javaClass.simpleName,
                dlqException,
            )
        }
    }

    internal fun resolveLsn(messageChange: MessageChange, fallback: LogSequenceNumber): LogSequenceNumber {
        val embedded = messageChange.lsn ?: return fallback
        // pgjdbc's LogSequenceNumber.valueOf never throws — it returns
        // INVALID_LSN (0/0) on a malformed input. We have to detect that
        // explicitly. INVALID_LSN is never a legitimate WAL position for a
        // real record, so falling back when we see it is safe.
        val parsed = LogSequenceNumber.valueOf(embedded)
        if (parsed == LogSequenceNumber.INVALID_LSN) {
            logger.warn(
                "Embedded wal2json lsn '{}' parsed as INVALID_LSN; falling back to stream LSN {}",
                embedded,
                fallback,
            )
            return fallback
        }
        return parsed
    }

    private fun parsePrefix(prefix: String): Pair<DestinationType, String> {
        val parts = prefix.split(PREFIX_SEPARATOR)
        return when (parts.size) {
            1 -> Pair(DestinationType.SNS, parts.first())
            2 -> Pair(DestinationType.valueOf(parts.first()), parts.last())
            else -> throw IllegalArgumentException("Invalid prefix: $prefix")
        }
    }

    private fun processSNSMessage(destinationName: String, messageChange: MessageChange, lsn: LogSequenceNumber) {
        val message = messageChange.content.parseToObject()
        logger.info(
            "Posting message to SNS topic #{} event type #{}, domainID #{}",
            destinationName,
            message.body.eventType,
            message.body.domainId,
        )
        val started = System.nanoTime()
        snsProducer.send(destinationName, message)
        metrics.recordPublishDuration(SlotReaderCallback.SINK_SNS, elapsedSince(started))
        slotReaderCallback.onSNSSuccess(lsn, destinationName, message)
    }

    private fun processSQSMessage(destinationName: String, messageChange: MessageChange, lsn: LogSequenceNumber) {
        logger.info("Posting message to SQS queue #{} with payload {}", destinationName, messageChange.content)
        val map = JsonHelper.fromJsonString(messageChange.content).toMap()
        val started = System.nanoTime()
        sqsProducer.send(destinationName, map)
        metrics.recordPublishDuration(SlotReaderCallback.SINK_SQS, elapsedSince(started))
        slotReaderCallback.onSQSSuccess(lsn, destinationName)
    }

    private fun elapsedSince(startNanos: Long): Duration =
        Duration.ofNanos(System.nanoTime() - startNanos)

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlotReaderMessageProducer::class.java)
        private const val RECOVERY_MODE_SQL_STATE = "57P03"
        private const val RECOVERY_MODE_SLEEP_MILLIS = 5000L
        private const val PREFIX_SEPARATOR = "|"
        private const val KIND_MESSAGE = "message"
        private const val KIND_EMPTY = "empty"
        private const val REASON_SQL = "sql_exception"
        private const val REASON_IO = "io_exception"
        private const val REASON_UNKNOWN = "unknown"
        private const val REASON_DEAD_LETTERED = "dead_lettered"
        const val DEFAULT_MAX_RECONNECT_ATTEMPTS = 30
        const val DEFAULT_MAX_PUBLISH_ATTEMPTS = 5
        const val DEFAULT_PUBLISH_BACKOFF_INITIAL_MS = 100L
        const val DEFAULT_PUBLISH_BACKOFF_MAX_SECONDS = 5L

        @Suppress("UNCHECKED_CAST")
        private fun String.parseToObject(): SNSMessage<Any> {
            val snsMessage = defaultMapper.readValue(this, SNSMessage::class.java)
            return snsMessage as SNSMessage<Any>
        }
    }
}
