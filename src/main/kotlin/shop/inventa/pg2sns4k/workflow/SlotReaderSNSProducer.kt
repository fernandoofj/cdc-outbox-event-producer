package shop.inventa.pg2sns4k.workflow

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessage
import shop.inventa.pg2sns4k.jackson.ObjectMapperSingleton.defaultMapper
import shop.inventa.pg2sns4k.replication.config.PostgresConfiguration
import shop.inventa.pg2sns4k.replication.config.ReplicationConfiguration
import shop.inventa.pg2sns4k.replication.connector.DefaultConnectionProvider
import shop.inventa.pg2sns4k.replication.connector.PostgresConnector
import shop.inventa.pg2sns4k.replication.model.MessageChange
import shop.inventa.pg2sns4k.replication.strategy.ByteToClassParserImplV1
import shop.inventa.pg2sns4k.replication.strategy.ByteToClassParserImplV2
import shop.inventa.pg2sns4k.replication.strategy.ByteToClassParserStrategy
import java.io.IOException
import java.nio.ByteBuffer
import java.sql.SQLException
import java.util.concurrent.TimeUnit

@Suppress("TooGenericExceptionCaught", "TooManyFunctions")
class SlotReaderSNSProducer(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration,
    private val snsTransactionalProducer: shop.inventa.pg2sns4k.aws.sns.SNSTransactionalProducer,
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
            readSlotWriteToSNS()
        }
    }

    fun resetIdleCounter() {
        lastFlushedTime = System.currentTimeMillis()
    }

    private fun readSlotWriteToSNS() {
        try {
            createPostgresConnector(postgresConfiguration, replicationConfiguration).use { postgresConnector ->
                initializeCallback(postgresConnector)
                resetIdleCounter()
                logger.info("Consuming from slot {}", replicationConfiguration.slotName)
                while (running) {
                    readSlotWriteToSNSHelper(postgresConnector)
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

    fun stopStreaming() {
        running = false
    }

    private fun initializeCallback(postgresConnector: PostgresConnector) {
        slotReaderCallback = SlotReaderCallback(
            this,
            postgresConnector
        )
    }

    private fun readSlotWriteToSNSHelper(postgresConnector: PostgresConnector) {
        var msg = postgresConnector.readPending()

        msg?.let {
            processReadedData(it)
        } ?: run {
            val currentTimeMillis = System.currentTimeMillis()
            val updateIdleSlotIntervalMillis =
                TimeUnit.SECONDS.toMillis(replicationConfiguration.updateIdleSlotInterval)

            if (currentTimeMillis - lastFlushedTime > updateIdleSlotIntervalMillis) {
                val lsn = postgresConnector.currentLSN()
                msg = postgresConnector.readPending()
                msg?.let {
                    processReadedData(it)
                }
                logger.info("Fast forwarding stream lsn to $lsn due to stream inactivity")
                postgresConnector.setStreamLsn(lsn)
                resetIdleCounter()
            }
        }
    }

    private fun processReadedData(byteBufferMessage: ByteBuffer) {
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
        val topicName = messageChange.prefix

        try {
            val message = messageChange.content.toJson()

            logger.info(
                "Posting event #${message.body.eventType} for domainID #${message.body.domainId} to topic #$topicName"
            )

            snsTransactionalProducer.send(topicName, message)
            slotReaderCallback.onSuccess(topicName, message)
        } catch (e: Exception) {
            slotReaderCallback.onFailure(topicName, e)
        }
    }

    @Suppress("TooGenericExceptionThrown", "UNCHECKED_CAST")
    private fun String.toJson(): SNSMessage<Any> {
        val snsMessage = defaultMapper.readValue(this, SNSMessage::class.java)
        return snsMessage as SNSMessage<Any>
    }

    private fun createPostgresConnector(
        postgresConfiguration: PostgresConfiguration,
        replicationConfiguration: ReplicationConfiguration
    ): PostgresConnector {
        return PostgresConnector(postgresConfiguration, replicationConfiguration, DefaultConnectionProvider())
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlotReaderSNSProducer::class.java)
        private const val RECOVERY_MODE_SQL_STATE = "57P03"
        private const val RECOVERY_MODE_SLEEP_MILLIS = 5000L
    }
}
