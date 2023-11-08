package shop.inventa.pg2sns4k.workflow

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shop.inventa.pg2sns4k.aws.sns.dto.SNSMessage
import shop.inventa.pg2sns4k.replication.config.PostgresConfiguration
import shop.inventa.pg2sns4k.replication.config.ReplicationConfiguration
import shop.inventa.pg2sns4k.replication.connector.DefaultConnectionProvider
import shop.inventa.pg2sns4k.replication.connector.PostgresConnector
import shop.inventa.pg2sns4k.replication.model.MessageChange
import shop.inventa.pg2sns4k.replication.model.SlotMessage
import java.io.IOException
import java.nio.ByteBuffer
import java.sql.SQLException
import java.util.concurrent.TimeUnit

@Suppress("TooGenericExceptionCaught", "TooManyFunctions")
class SlotReaderSNSProducer(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration,
    private val snsTransactionalProducer: shop.inventa.pg2sns4k.aws.sns.SNSTransactionalProducer,
    private val isTestingExecution: Boolean = false
) {
    private var running = true
    private var lastFlushedTime: Long = 0
    private var slotReaderCallback: SlotReaderCallback? = null

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
                    val didReadMessageSuccessfully = readSlotWriteToSNSHelper(postgresConnector)
                    checkIfNeedsToStopRunning(didReadMessageSuccessfully)
                }
            }
        } catch (sqlException: SQLException) {
            logger.error(
                "Received the following error pertaining to the replication stream, reattempting...",
                sqlException
            )
            if (sqlException.sqlState == RECOVERY_MODE_SQL_STATE) {
                logger.info("Sleeping for five seconds")
                try {
                    Thread.sleep(RECOVERY_MODE_SLEEP_MILLIS)
                } catch (interruptedException: InterruptedException) {
                    logger.error("Interrupted while sleeping", interruptedException)
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

    private fun checkIfNeedsToStopRunning(didReadMessageSuccessfully: Boolean) {
        if (isTestingExecution && didReadMessageSuccessfully) {
            stopStreaming()
        }
    }

    private fun stopStreaming() {
        running = false
    }

    private fun initializeCallback(postgresConnector: PostgresConnector) {
        if (slotReaderCallback == null) {
            slotReaderCallback = SlotReaderCallback(
                this,
                postgresConnector
            )
        }
    }

    private fun readSlotWriteToSNSHelper(postgresConnector: PostgresConnector): Boolean {
        var isMessageReaded = false

        var msg = postgresConnector.readPending()

        msg?.let {
            processReadedData(it)
            isMessageReaded = true
        } ?: run {
            val currentTimeMillis = System.currentTimeMillis()
            val updateIdleSlotIntervalMillis =
                TimeUnit.SECONDS.toMillis(replicationConfiguration.updateIdleSlotInterval)

            if (currentTimeMillis - lastFlushedTime > updateIdleSlotIntervalMillis) {
                val lsn = postgresConnector.currentLSN()
                msg = postgresConnector.readPending()
                msg?.let {
                    processReadedData(it)
                    isMessageReaded = true
                }
                logger.info("Fast forwarding stream lsn to $lsn due to stream inactivity")
                postgresConnector.setStreamLsn(lsn)
                resetIdleCounter()
            }
        }
        return isMessageReaded
    }

    private fun processReadedData(byteBufferMessage: ByteBuffer) {
        val slotMessage = transformByteBufferToSlotMessage(byteBufferMessage)
        slotMessage.changes.forEach { change ->
            when (change.kind) {
                "message" -> processMessage(change as MessageChange)
                else -> slotReaderCallback?.discardMessage(change.kind)
            }
        }
    }

    private fun transformByteBufferToSlotMessage(byteBufferMessage: ByteBuffer): SlotMessage {
        val byteArray = ByteArray(byteBufferMessage.remaining())
        byteBufferMessage.get(byteArray)
        val jsonString = String(byteArray, Charsets.UTF_8)
        return defaultMapper().readValue(jsonString, SlotMessage::class.java)
    }

    private fun processMessage(messageChange: MessageChange) {
        val topicName = messageChange.prefix

        try {
            val message = messageChange.content.toJson()

            logger.info("Posting msg $messageChange to topic $topicName")

            snsTransactionalProducer.send(topicName, message)

            slotReaderCallback?.onSuccess(topicName, message.body.eventType, message.body.eventUUID)
        } catch (e: Exception) {
            slotReaderCallback?.onFailure(topicName, e)
        }
    }

    @Suppress("TooGenericExceptionThrown", "UNCHECKED_CAST")
    private fun String.toJson(): SNSMessage<Any> {
        val snsMessage = defaultMapper().readValue(this, SNSMessage::class.java)
        return snsMessage as SNSMessage<Any>
    }

    private fun createPostgresConnector(
        postgresConfiguration: PostgresConfiguration,
        replicationConfiguration: ReplicationConfiguration
    ): PostgresConnector {
        return PostgresConnector(postgresConfiguration, replicationConfiguration, DefaultConnectionProvider())
    }

    companion object {
        private fun defaultMapper(): ObjectMapper {
            val objectMapper = ObjectMapper()
            objectMapper.registerModule(JavaTimeModule())
            objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            return objectMapper
        }

        private val logger: Logger = LoggerFactory.getLogger(SlotReaderSNSProducer::class.java)
        private const val RECOVERY_MODE_SQL_STATE = "57P03"
        private const val RECOVERY_MODE_SLEEP_MILLIS = 5000L
    }
}
