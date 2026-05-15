package br.com.fltech.cdc.outbox.publisher.replication.config

import br.com.fltech.cdc.outbox.publisher.replication.enums.FormatVersionEnum
import java.util.Properties
import java.util.concurrent.TimeUnit

data class ReplicationConfiguration(
    val slotName: String,
    val outputPlugin: String = DEFAULT_OUTPUT_PLUGIN,
    val statusIntervalTimeUnit: TimeUnit = DEFAULT_STATUS_INTERVAL_TIME_UNIT,
    val statusIntervalValue: Int = DEFAULT_STATUS_INTERVAL_VALUE,
    val updateIdleSlotInterval: Long = DEFAULT_UPDATE_IDLE_SLOT_INTERVAL,
    val existingProcessRetryLimit: Int? = DEFAULT_EXISTING_PROCESS_RETRY_LIMIT,
    val existingProcessRetrySleepSeconds: Long? = DEFAULT_EXISTING_PROCESS_RETRY_SLEEP_SECONDS,
    val includeXids: Boolean = DEFAULT_INCLUDE_XIDS,
    /**
     * Request the wal2json output plugin to embed the LSN of each WAL record
     * in the JSON payload (top-level `lsn` field). The producer prefers this
     * value over `PGReplicationStream.lastReceiveLSN`, which is the stream's
     * high-water mark and may drift ahead of the message being processed.
     * Disable only if you are running a wal2json build that pre-dates the
     * option (very old; the option exists since 1.x).
     */
    val includeLsn: Boolean = DEFAULT_INCLUDE_LSN,
    val formatVersion: FormatVersionEnum = DEFAULT_FORMAT_VERSION,
) {
    fun getSlotOptions(): Properties {
        val properties = Properties()
        properties.setProperty("include-xids", includeXids.toString())
        properties.setProperty("include-lsn", includeLsn.toString())
        properties.setProperty("format-version", formatVersion.code)
        return properties
    }

    companion object {
        const val DEFAULT_STATUS_INTERVAL_VALUE = 20
        const val DEFAULT_INCLUDE_XIDS = true
        const val DEFAULT_INCLUDE_LSN = true
        const val DEFAULT_OUTPUT_PLUGIN = "wal2json"
        const val DEFAULT_UPDATE_IDLE_SLOT_INTERVAL = 300L
        const val DEFAULT_EXISTING_PROCESS_RETRY_LIMIT = 30
        const val DEFAULT_EXISTING_PROCESS_RETRY_SLEEP_SECONDS = 30L
        val DEFAULT_FORMAT_VERSION = FormatVersionEnum.V2
        val DEFAULT_STATUS_INTERVAL_TIME_UNIT = TimeUnit.SECONDS
    }
}
