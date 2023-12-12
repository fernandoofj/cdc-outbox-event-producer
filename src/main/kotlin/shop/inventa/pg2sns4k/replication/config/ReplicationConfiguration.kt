package shop.inventa.pg2sns4k.replication.config

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
    val formatVersion: String = DEFAULT_FORMAT_VERSION
) {
    fun getSlotOptions(): Properties {
        val properties = Properties()
        properties.setProperty("include-xids", includeXids.toString())
        properties.setProperty("format-version", formatVersion)
        return properties
    }

    companion object {
        const val DEFAULT_STATUS_INTERVAL_VALUE = 20
        const val DEFAULT_INCLUDE_XIDS = true
        const val DEFAULT_OUTPUT_PLUGIN = "wal2json"
        const val DEFAULT_UPDATE_IDLE_SLOT_INTERVAL = 300L
        const val DEFAULT_EXISTING_PROCESS_RETRY_LIMIT = 30
        const val DEFAULT_EXISTING_PROCESS_RETRY_SLEEP_SECONDS = 30L
        const val DEFAULT_FORMAT_VERSION = "2"
        val DEFAULT_STATUS_INTERVAL_TIME_UNIT = TimeUnit.SECONDS
    }
}
