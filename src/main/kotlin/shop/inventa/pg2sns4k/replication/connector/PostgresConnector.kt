package shop.inventa.pg2sns4k.replication.connector

import org.postgresql.PGConnection
import org.postgresql.replication.LogSequenceNumber
import org.postgresql.replication.PGReplicationConnection
import org.postgresql.replication.PGReplicationStream
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import shop.inventa.pg2sns4k.replication.config.PostgresConfiguration
import shop.inventa.pg2sns4k.replication.config.ReplicationConfiguration
import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.SQLException
import java.util.Properties
import java.util.concurrent.TimeUnit
import kotlin.math.pow

@Suppress("TooManyFunctions")
open class PostgresConnector(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration,
    private val connectionProvider: ConnectionProvider
) : AutoCloseable {
    private val queryConnection: Connection?
    private val streamingConnection: Connection?
    private val pgReplicationStream: PGReplicationStream?

    init {
        logger.debug("Connecting to {}", postgresConfiguration.getUrl())
        queryConnection =
            createConnection(postgresConfiguration.getUrl(), postgresConfiguration.getQueryConnectionProperties())
        streamingConnection =
            createConnection(postgresConfiguration.getUrl(), postgresConfiguration.getReplicationProperties())
        logger.debug("Connected to postgres")
        val pgConnection = streamingConnection.unwrap(PGConnection::class.java)
        val pgReplicationConnection = pgConnection.replicationAPI
        try {
            logger.info("Attempting to create replication slot ${replicationConfiguration.slotName}")
            pgReplicationConnection.createReplicationSlot()
                .logical()
                .withOutputPlugin(replicationConfiguration.outputPlugin)
                .withSlotName(replicationConfiguration.slotName)
                .make()
            logger.info("Created replication slot")
        } catch (e: SQLException) {
            when (e.sqlState) {
                ALREADY_EXISTS_SQL_STATE -> logger.info("Slot ${replicationConfiguration.slotName} already exists")
                else -> throw e
            }
        }
        pgReplicationStream = getPgReplicationStream(replicationConfiguration, pgReplicationConnection)
    }

    fun readPending(): ByteBuffer? = pgReplicationStream?.let { it.readPending() }

    fun currentLSN(): LogSequenceNumber {
        queryConnection?.let {
            val statement = it.createStatement()
            val resultSet = statement.executeQuery(CURRENT_WAL_LSN_QUERY)
            if (resultSet.next()) {
                val lsn: String = resultSet.getString(1)
                return LogSequenceNumber.valueOf(lsn)
            }
        }

        return LogSequenceNumber.INVALID_LSN
    }

    fun setStreamLsn(lsn: LogSequenceNumber) {
        pgReplicationStream?.let {
            it.setAppliedLSN(lsn)
            it.setFlushedLSN(lsn)
        }
    }

    fun lastReceivedLsn(): LogSequenceNumber = pgReplicationStream!!.lastReceiveLSN

    override fun close() {
        pgReplicationStream?.let {
            try {
                if (!it.isClosed) {
                    it.forceUpdateStatus()
                    it.close()
                }
            } catch (sqlException: SQLException) {
                logger.error("Unable to close replication stream", sqlException)
            }
        }
        streamingConnection?.let {
            try {
                it.close()
            } catch (sqlException: SQLException) {
                logger.error("Unable to close postgres streaming connection", sqlException)
            }
        }
        queryConnection?.let {
            try {
                it.close()
            } catch (sqlException: SQLException) {
                logger.error("Unable to close postgres query connection", sqlException)
            }
        }
    }

    private fun createConnection(url: String, properties: Properties): Connection {
        return connectionProvider.getConnection(url, properties)
    }

    private fun getPgReplicationStream(
        replicationConfiguration: ReplicationConfiguration,
        pgReplicationConnection: PGReplicationConnection
    ): PGReplicationStream? {
        val maxRetries = replicationConfiguration.existingProcessRetryLimit
        var listening = false
        var tries = 1
        var pgRepStream: PGReplicationStream? = null

        while (!listening && tries.canContinue(maxRetries)) {
            try {
                pgRepStream = getPgReplicationStreamHelper(replicationConfiguration, pgReplicationConnection)
                listening = true
            } catch (psqlException: PSQLException) {
                when (psqlException.sqlState) {
                    CURRENTLY_RUNNING_PROCESS_ON_SLOT_SQL_STATE -> {
                        tries = handleCurrentlyRunningProcessOnSlotException(tries, maxRetries)
                    }
                    else -> throw psqlException
                }
            }
        }
        return pgRepStream
    }

    private fun Int.canContinue(maxRetries: Int?) = maxRetries == null || this <= maxRetries

    private fun handleCurrentlyRunningProcessOnSlotException(tries: Int, maxRetries: Int?): Int {
        logger.info("Replication slot currently has another process consuming from it")

        val delayTime = replicationConfiguration.existingProcessRetrySleepSeconds
            ?: calculateExponentialDelay(tries - 1)

        logger.info("Sleeping for $delayTime seconds before retry $tries/${maxRetries ?: "∞"}")

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(delayTime))
        } catch (ie: InterruptedException) {
            logger.info("Received interruption while attempting to setup replciation stream")
        }

        return tries + 1
    }

    private fun calculateExponentialDelay(tries: Int) =
        minOf(
            (EXPONENTIAL_BASE_DELAY_SECONDS * EXPONENTIAL_BINARY_BASE.pow(tries)).toLong(),
            EXPONENTIAL_MAX_DELAY_SECONDS
        )

    private fun getPgReplicationStreamHelper(
        replicationConfiguration: ReplicationConfiguration,
        pgReplicationConnection: PGReplicationConnection
    ): PGReplicationStream {
        return pgReplicationConnection
            .replicationStream()
            .logical()
            .withStatusInterval(
                replicationConfiguration.statusIntervalValue,
                replicationConfiguration.statusIntervalTimeUnit
            )
            .withSlotOptions(replicationConfiguration.getSlotOptions())
            .withSlotName(replicationConfiguration.slotName).start()
    }

    companion object {
        private const val ALREADY_EXISTS_SQL_STATE = "42710"
        private const val CURRENTLY_RUNNING_PROCESS_ON_SLOT_SQL_STATE = "55006"
        private const val CURRENT_WAL_LSN_QUERY = "select pg_current_wal_lsn()"
        private const val EXPONENTIAL_MAX_DELAY_SECONDS = (5 * 60).toLong()
        private const val EXPONENTIAL_BASE_DELAY_SECONDS = 30
        private const val EXPONENTIAL_BINARY_BASE = 2.0
        private val logger = LoggerFactory.getLogger(PostgresConnector::class.java)
    }
}
