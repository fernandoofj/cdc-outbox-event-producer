package shop.inventa.pg2sns4k.common.workflow

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.postgresql.replication.LogSequenceNumber
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shop.inventa.pg2sns4k.common.aws.sns.SNSTransactionalProducer
import shop.inventa.pg2sns4k.common.aws.sns.dto.SNSMessage
import shop.inventa.pg2sns4k.common.replication.config.PostgresConfiguration
import shop.inventa.pg2sns4k.common.replication.config.ReplicationConfiguration
import shop.inventa.pg2sns4k.common.replication.connector.DefaultConnectionProvider
import shop.inventa.pg2sns4k.common.replication.connector.PostgresConnector
import shop.inventa.pg2sns4k.common.replication.model.MessageChange
import shop.inventa.pg2sns4k.common.replication.model.SlotMessage
import java.io.IOException
import java.nio.ByteBuffer
import java.sql.SQLException
import java.util.concurrent.TimeUnit

@Suppress("TooGenericExceptionCaught")
class SlotReaderSNSProducer(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration,
    private val snsTransactionalProducer: SNSTransactionalProducer
) {
    private var lastFlushedTime: Long = 0
    private var slotReaderCallback: SlotReaderCallback? = null

    fun startStreaming() {
        while (true) {
            readSlotWriteToSNS()
        }
    }

    fun resetIdleCounter() {
        lastFlushedTime = System.currentTimeMillis()
    }

    private fun readSlotWriteToSNS() {
        try {
            createPostgresConnector(
                postgresConfiguration, replicationConfiguration
            ).use { postgresConnector ->
                initializeCallback(postgresConnector)
                resetIdleCounter()
                logger.info("Consuming from slot {}", replicationConfiguration.slotName)
                while (true) {
                    readSlotWriteToSNSHelper(postgresConnector)
                }
            }
        } catch (sqlException: SQLException) {
            logger.error(
                "Received the following error pertaining to the replication stream, reattempting...", sqlException
            )
            if (sqlException.sqlState == RECOVERY_MODE_SQL_STATE) {
                logger.info("Sleeping for five seconds")
                try {
                    Thread.sleep(RECOVERY_MODE_SLEEP_MILLIS)
                } catch (ie: InterruptedException) {
                    logger.error("Interrupted while sleeping", ie)
                }
            }
        } catch (ioException: IOException) {
            logger.error(
                "Received an IO Exception while processing the replication stream, reattempting...", ioException
            )
        } catch (e: Exception) {
            logger.error("Received exception of type {}", e.javaClass.toString(), e)
        }
    }

    private fun initializeCallback(postgresConnector: PostgresConnector) {
        if (slotReaderCallback == null) {
            slotReaderCallback = SlotReaderCallback(
                this,
                postgresConnector
            )
        }
    }

    @Throws(SQLException::class, IOException::class)
    private fun readSlotWriteToSNSHelper(
        postgresConnector: PostgresConnector
    ) {
        var msg = postgresConnector.readPending()
        if (msg != null) {
            processByteBuffer(msg)
        } else if (
            System.currentTimeMillis() - lastFlushedTime
        > TimeUnit.SECONDS.toMillis(replicationConfiguration.updateIdleSlotInterval)
        ) {
            val lsn: LogSequenceNumber = postgresConnector.currentLSN()
            msg = postgresConnector.readPending()
            msg?.let { processByteBuffer(it) }
            logger.info("Fast forwarding stream lsn to {} due to stream inactivity", lsn.toString())
            postgresConnector.setStreamLsn(lsn)
            resetIdleCounter()
        }
    }

    @Throws(IOException::class)
    private fun processByteBuffer(
        msg: ByteBuffer
    ) {
        val offset = msg.arrayOffset()
        val source = msg.array()
        val slotMessage: SlotMessage = getSlotMessage(source, offset)
        if (slotMessage.changes.isNotEmpty()) {
            slotMessage.changes.forEach { change ->
                when (change.kind) {
                    "message" -> processMessage(change as MessageChange)
                    else -> slotReaderCallback?.discardMessage(change.kind)
                }
            }
        }
    }

    private fun processMessage(messageChange: MessageChange) {
        val topicName = messageChange.prefix

        try {
            val message = messageChange.content.toJson()

            logger.info("Posting msg {} to topic {}", message.toString(), topicName)

            snsTransactionalProducer.send(topicName, message)
            slotReaderCallback?.onSuccess(topicName, message.body.eventType, message.body.eventUUID)
        } catch (e: Exception) {
            slotReaderCallback?.onFailure(topicName, e)
        }
    }

    @Suppress("TooGenericExceptionThrown", "UNCHECKED_CAST")
    private fun String.toJson(): SNSMessage<Any> {
        val snsMessage = defaultMapper().readValue(this, SNSMessage::class.java)

        if (snsMessage is SNSMessage<*>) {
            return snsMessage as SNSMessage<Any>
        } else {
            throw RuntimeException("Failed to parse message")
        }
    }

    @Throws(IOException::class)
    private fun getSlotMessage(walChunk: ByteArray, offset: Int): SlotMessage {
        return defaultMapper().readValue(walChunk, offset, walChunk.size, SlotMessage::class.java)
    }

    @Throws(SQLException::class)
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
