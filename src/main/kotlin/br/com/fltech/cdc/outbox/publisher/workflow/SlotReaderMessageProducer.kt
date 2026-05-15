package br.com.fltech.cdc.outbox.publisher.workflow

import br.com.fltech.cdc.outbox.publisher.aws.sns.SNSProducer
import br.com.fltech.cdc.outbox.publisher.aws.sns.dto.SNSMessage
import br.com.fltech.cdc.outbox.publisher.aws.sqs.SQSProducer
import br.com.fltech.cdc.outbox.publisher.helper.JsonHelper
import br.com.fltech.cdc.outbox.publisher.jackson.ObjectMapperSingleton.defaultMapper
import br.com.fltech.cdc.outbox.publisher.replication.config.PostgresConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.config.ReplicationConfiguration
import br.com.fltech.cdc.outbox.publisher.replication.connector.DefaultConnectionProvider
import br.com.fltech.cdc.outbox.publisher.replication.connector.PostgresConnector
import br.com.fltech.cdc.outbox.publisher.replication.model.MessageChange
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserImplV1
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserImplV2
import br.com.fltech.cdc.outbox.publisher.replication.strategy.ByteToClassParserStrategy
import java.io.IOException
import java.nio.ByteBuffer
import java.sql.SQLException
import java.util.concurrent.TimeUnit
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Suppress("TooGenericExceptionCaught", "TooManyFunctions")
class SlotReaderMessageProducer(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration,
    private val snsProducer: SNSProducer,
    private val sqsProducer: SQSProducer
) {
    private var running = true
    private var lastFlushedTime: Long = 0
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

    private fun readingSlotData() {
        try {
            createPostgresConnector(postgresConfiguration, replicationConfiguration).use { postgresConnector ->
                initializeCallback(postgresConnector)
                resetIdleCounter()
                logger.info("Consuming from slot {}", replicationConfiguration.slotName)
                while (running) {
                    readDataPostingToDestination(postgresConnector)
                }
            }
        } catch (sqlException: SQLException) {
            logger.error(
                "Received the following error pertaining to the replication stream, reattempting...",
                sqlException
            )
            when (sqlException.sqlState) {
                RECOVERY_MODE_SQL_STATE -> {
                    logger.info("Sleeping for five seconds")
                    try {
                        Thread.sleep(RECOVERY_MODE_SLEEP_MILLIS)
                    } catch (interruptedException: InterruptedException) {
                        logger.error("Interrupted while sleeping", interruptedException)
                    }
                }
            }
        } catch (ioException: IOException) {
            logger.error(
                "Received an IO Exception while processing the replication stream, reattempting...",
                ioException
            )
        } catch (exception: Exception) {
            logger.error("Received exception of type ${exception.javaClass}", exception)
        }
    }

    private fun createPostgresConnector(
        postgresConfiguration: PostgresConfiguration,
        replicationConfiguration: ReplicationConfiguration
    ): PostgresConnector {
        return PostgresConnector(postgresConfiguration, replicationConfiguration, DefaultConnectionProvider())
    }

    private fun initializeCallback(postgresConnector: PostgresConnector) {
        slotReaderCallback = SlotReaderCallback(
            this,
            postgresConnector
        )
    }

    private fun readDataPostingToDestination(postgresConnector: PostgresConnector) {
        var msg = postgresConnector.readPending()

        msg?.let {
            processData(it)
        } ?: run {
            val currentTimeMillis = System.currentTimeMillis()
            val updateIdleSlotIntervalMillis =
                TimeUnit.SECONDS.toMillis(replicationConfiguration.updateIdleSlotInterval)

            if (currentTimeMillis - lastFlushedTime > updateIdleSlotIntervalMillis) {
                val lsn = postgresConnector.currentLSN()
                msg = postgresConnector.readPending()
                msg?.let {
                    processData(it)
                }
                logger.info("Fast forwarding stream lsn to $lsn due to stream inactivity")
                postgresConnector.setStreamLsn(lsn)
                resetIdleCounter()
            }
        }
    }

    private fun processData(byteBufferMessage: ByteBuffer) {
        val changes = byteToClassParser.parse(byteBufferMessage)
        changes.takeIf { it.isNotEmpty() }?.forEach { change ->
            when (change.kind) {
                "message" -> processMessage(change as MessageChange)
                else -> slotReaderCallback.discardMessage(change.kind)
            }
        } ?: run {
            slotReaderCallback.discardMessage("empty")
        }
    }

    private fun processMessage(messageChange: MessageChange) {
        try {
            logger.info("Processing message for prefix ${messageChange.prefix} with content ${messageChange.content}")

            val prefixPair = parsePrefix(messageChange.prefix)
            val destinationName = prefixPair.second

            when (prefixPair.first) {
                DestinationType.SNS -> processSNSMessage(destinationName, messageChange)
                DestinationType.SQS -> processSQSMessage(destinationName, messageChange)
            }
        } catch (e: Exception) {
            slotReaderCallback.onFailure(messageChange.prefix, e)
        } finally {
            logger.info("Processed message for prefix ${messageChange.prefix}")
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

    private fun processSNSMessage(destinationName: String, messageChange: MessageChange) {
        val message = messageChange.content.parseToObject()

        logger.info(
            "Posting message to SNS topic #$destinationName " +
                "event type #${message.body.eventType}, " +
                "domainID #${message.body.domainId} " +
                "with payload ${message.body}"
        )

        snsProducer.send(destinationName, message)
        slotReaderCallback.onSNSSuccess(destinationName, message)
    }

    private fun processSQSMessage(destinationName: String, messageChange: MessageChange) {
        logger.info(
            "Posting message to SQS queue #$destinationName with payload ${messageChange.content}"
        )

        val map = JsonHelper.fromJsonString(messageChange.content).toMap()

        sqsProducer.send(destinationName, map)
        slotReaderCallback.onSQSSuccess(destinationName)
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlotReaderMessageProducer::class.java)
        private const val RECOVERY_MODE_SQL_STATE = "57P03"
        private const val RECOVERY_MODE_SLEEP_MILLIS = 5000L
        private const val PREFIX_SEPARATOR = "|"

        @Suppress("TooGenericExceptionThrown", "UNCHECKED_CAST")
        private fun String.parseToObject(): SNSMessage<Any> {
            val snsMessage = defaultMapper.readValue(this, SNSMessage::class.java)
            return snsMessage as SNSMessage<Any>
        }
    }
}
