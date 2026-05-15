package br.com.fltech.cdc.outbox.publisher.workflow

import br.com.fltech.cdc.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.cdc.outbox.publisher.aws.sqs.SQSProducer
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
 * Known limitation (tracked as Wave 1.5): the per-message LSN we thread
 * through the callbacks is obtained from `PGReplicationStream.lastReceiveLSN`
 * immediately after `readPending()` returns. That value is the stream's
 * high-water mark, so in the rare case where the driver has already
 * buffered message N+1 while we are still reading N, the captured LSN is
 * one message ahead. The genuine per-message LSN must be parsed from the
 * wal2json payload's `nextlsn` field (or the pgoutput protocol header) —
 * that follow-up belongs in the multi-DB wave.
 */
// TooManyFunctions: this class is the orchestrator. The functions are
// short, focused, and each plays a distinct role in the streaming loop.
// Will be split into application/CdcProcessor + adapter when the
// hexagonal refactor lands (Wave 3+).
@Suppress("TooManyFunctions")
class SlotReaderMessageProducer(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration,
    private val snsProducer: SNSProducer,
    private val sqsProducer: SQSProducer,
    private val connectionProvider: ConnectionProvider = HikariCPConnectionProvider(),
    private val metrics: CdcOutboxMetrics = CdcOutboxMetrics.noop(),
    private val reconnectBackOff: BackOff = ExponentialBackOff(),
    private val maxReconnectAttempts: Int = DEFAULT_MAX_RECONNECT_ATTEMPTS,
) {
    @Volatile
    private var running = true

    private var lastFlushedTime: Long = 0
    private var reconnectAttempt: Int = 0

    /**
     * LSN of the last message whose publish failed and has NOT yet been
     * redelivered successfully. While this is non-null the idle-flush path
     * MUST NOT fast-forward the slot, otherwise Postgres would recycle WAL
     * past the failed message and break the at-least-once contract.
     */
    private var pendingFailureLsn: LogSequenceNumber? = null

    private lateinit var slotReaderCallback: SlotReaderCallback
    private val byteToClassParserImplV1 = ByteToClassParserImplV1(defaultMapper)
    private val byteToClassParserImplV2 = ByteToClassParserImplV2(defaultMapper)
    private val byteToClassParser = ByteToClassParserStrategy(byteToClassParserImplV1, byteToClassParserImplV2)
        .selectParser(replicationConfiguration)

    fun startStreaming() {
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

    private fun initializeCallback(postgresConnector: PostgresConnector) {
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
    // not crash the streaming loop. The captured throwable is forwarded to
    // onFailure for logging + metrics, and the slot stays put for redelivery.
    @Suppress("TooGenericExceptionCaught")
    private fun processMessage(messageChange: MessageChange, lsn: LogSequenceNumber) {
        val prefixPair = parsePrefix(messageChange.prefix)
        val destinationName = prefixPair.second
        val sink = prefixPair.first.name.lowercase()
        logger.info("Processing message for prefix {} (lsn {})", messageChange.prefix, lsn)
        try {
            when (prefixPair.first) {
                DestinationType.SNS -> processSNSMessage(destinationName, messageChange, lsn)
                DestinationType.SQS -> processSQSMessage(destinationName, messageChange, lsn)
            }
            // Successful publish: clear any prior pending failure marker if it
            // referred to this LSN (the message has now been redelivered and acked).
            //
            // Caveat: because `lsn` is currently the stream's high-water mark
            // (see class KDoc + Wave 1.5), a redelivered message may produce a
            // *different* captured LSN than the one that failed — so the flag
            // can stick across a successful redelivery. Symptom: extra "no idle
            // fast-forward" cycles until the loop restarts. This is conservative
            // (never advances past the failure) and disappears once the true
            // per-message LSN is parsed from `nextlsn` in Wave 1.5.
            if (pendingFailureLsn == lsn) {
                pendingFailureLsn = null
            }
        } catch (e: Exception) {
            // Remember the failing LSN so the idle-flush path does not jump past it.
            pendingFailureLsn = lsn
            slotReaderCallback.onFailure(lsn, messageChange.prefix, sink, e)
        }
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
        const val DEFAULT_MAX_RECONNECT_ATTEMPTS = 30

        @Suppress("UNCHECKED_CAST")
        private fun String.parseToObject(): SNSMessage<Any> {
            val snsMessage = defaultMapper.readValue(this, SNSMessage::class.java)
            return snsMessage as SNSMessage<Any>
        }
    }
}
