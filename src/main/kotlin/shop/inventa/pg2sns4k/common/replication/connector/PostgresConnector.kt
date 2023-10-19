package shop.inventa.pg2sns4k.common.replication.connector

import org.postgresql.PGConnection
import org.postgresql.replication.LogSequenceNumber
import org.postgresql.replication.PGReplicationConnection
import org.postgresql.replication.PGReplicationStream
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import shop.inventa.pg2sns4k.common.replication.config.PostgresConfiguration
import shop.inventa.pg2sns4k.common.replication.config.ReplicationConfiguration
import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.Properties
import java.util.concurrent.TimeUnit

class PostgresConnector(
    private val postgresConfiguration: PostgresConfiguration,
    private val replicationConfiguration: ReplicationConfiguration
) : AutoCloseable {
    private val queryConnection: Connection?
    private val streamingConnection: Connection?
    private val pgReplicationStream: PGReplicationStream?

    init {
        logger.debug("Connecting to {}", postgresConfiguration.getUrl())
        queryConnection = createConnection(
            postgresConfiguration.getUrl(),
            postgresConfiguration.getQueryConnectionProperties()
        )
        streamingConnection = createConnection(
            postgresConfiguration.getUrl(),
            postgresConfiguration.getReplicationProperties()
        )
        logger.debug("Connected to postgres")
        val pgConnection = streamingConnection.unwrap(PGConnection::class.java)
        val pgReplicationConnection = pgConnection.replicationAPI
        try {
            logger.info("Attempting to create replication slot {}", replicationConfiguration.slotName)
            pgReplicationConnection.createReplicationSlot()
                .logical()
                .withOutputPlugin(replicationConfiguration.outputPlugin)
                .withSlotName(replicationConfiguration.slotName)
                .make()
            logger.info("Created replication slot")
        } catch (e: SQLException) {
            when (e.sqlState) {
                ALREADY_EXISTS_SQL_STATE -> logger.info("Slot {} already exists", replicationConfiguration.slotName)
                else -> throw e
            }
        }
        pgReplicationStream = getPgReplicationStream(replicationConfiguration, pgReplicationConnection)
    }

    fun readPending(): ByteBuffer? {
        return pgReplicationStream!!.readPending()
    }

    fun currentLSN(): LogSequenceNumber {
        queryConnection!!.createStatement().use { st ->
            st.executeQuery(CURRENT_WAL_LSN_QUERY).use { rs ->
                if (rs.next()) {
                    val lsn: String = rs.getString(1)
                    return LogSequenceNumber.valueOf(lsn)
                }
            }
        }

        return LogSequenceNumber.INVALID_LSN
    }

    fun setStreamLsn(lsn: LogSequenceNumber?) {
        pgReplicationStream!!.setAppliedLSN(lsn)
        pgReplicationStream.setFlushedLSN(lsn)
    }

    fun lastReceivedLsn(): LogSequenceNumber = pgReplicationStream!!.lastReceiveLSN

    override fun close() {
        if (pgReplicationStream != null) {
            try {
                if (!pgReplicationStream.isClosed) {
                    pgReplicationStream.forceUpdateStatus()
                    pgReplicationStream.close()
                }
            } catch (sqlException: SQLException) {
                logger.error("Unable to close replication stream", sqlException)
            }
        }
        if (streamingConnection != null) {
            try {
                streamingConnection.close()
            } catch (sqlException: SQLException) {
                logger.error("Unable to close postgres streaming connection", sqlException)
            }
        }
        if (queryConnection != null) {
            try {
                queryConnection.close()
            } catch (sqlException: SQLException) {
                logger.error("Unable to close postgres query connection", sqlException)
            }
        }
    }

    private fun getPgReplicationStream(
        replicationConfiguration: ReplicationConfiguration,
        pgReplicationConnection: PGReplicationConnection
    ): PGReplicationStream? {
        var listening = false
        var tries: Int = replicationConfiguration.existingProcessRetryLimit
        var pgRepStream: PGReplicationStream? = null
        while (!listening && tries > 0) {
            try {
                pgRepStream = getPgReplicationStreamHelper(replicationConfiguration, pgReplicationConnection)
                listening = true
            } catch (psqlException: PSQLException) {
                when (psqlException.sqlState) {
                    CURRENTLY_RUNNING_PROCESS_ON_SLOT_SQL_STATE -> {
                        tries = handleCurrentlyRunningProcessOnSlotException(tries, psqlException)
                    }
                    else -> throw psqlException
                }
            }
        }
        return pgRepStream
    }

    private fun handleCurrentlyRunningProcessOnSlotException(currentTries: Int, psqlException: PSQLException): Int {
        logger.info("Replication slot currently has another process consuming from it")

        var tries = currentTries - 1

        if (tries > 0) {
            logger.info(
                "Sleeping for {} seconds before retrying {} more times",
                replicationConfiguration.existingProcessRetrySleepSeconds,
                tries,
                psqlException
            )
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(replicationConfiguration.existingProcessRetrySleepSeconds))
            } catch (ie: InterruptedException) {
                logger.info("Received interruption while attempting to setup replciation stream")
                tries = 0
            }
        }

        return tries
    }

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

    private fun createConnection(url: String?, properties: Properties?): Connection {
        return DriverManager.getConnection(url, properties)
    }

    companion object {
        private const val ALREADY_EXISTS_SQL_STATE = "42710"
        private const val CURRENTLY_RUNNING_PROCESS_ON_SLOT_SQL_STATE = "55006"
        private const val CURRENT_WAL_LSN_QUERY = "select pg_current_wal_lsn()"
        private val logger = LoggerFactory.getLogger(PostgresConnector::class.java)
    }
}
