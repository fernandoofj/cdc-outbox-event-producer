package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.core.port.CdcSource
import br.com.fltech.outbox.publisher.core.port.EventSinkRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.actuate.info.Info
import org.springframework.boot.actuate.info.InfoContributor

/**
 * Actuator `info` contributor that surfaces non-sensitive
 * configuration of the running cdc-outbox producer.
 *
 * Exposed shape under `/actuator/info`:
 * ```
 * {
 *   "cdc-outbox": {
 *     "version": "0.3.0",
 *     "processor": {"kind": "HEXAGONAL"},
 *     "replication": {"slot": "orders_outbox_slot", "outputPlugin": "wal2json"},
 *     "source": {"type": "PgLogicalReplicationCdcSource"},
 *     "sinks": {"schemes": ["sns", "sqs", "kafka"]},
 *     "mappings": {"count": 3},
 *     "checkpoint": {"enabled": true, "directory": ".cdc-outbox-checkpoints"},
 *     "lag": {"enabled": true, "interval": "PT10S"},
 *     "dlqReplay": {"enabled": true},
 *     "replay": {"enabled": false}
 *   }
 * }
 * ```
 *
 * **Sensitive data policy.** This contributor intentionally avoids
 * exposing anything that could be useful to an attacker:
 *  - No DB host / username / password (would leak the replication
 *    credentials).
 *  - No DLQ queue names (low-information leak about the AWS account
 *    layout — operators already have this via console).
 *  - No file paths beyond the checkpoint directory (which is non-
 *    secret operational metadata).
 *  - No mapping definitions, only the count.
 *
 * Property names use `kebab` rather than `snake` because that matches
 * the Spring conventions readers see elsewhere in Actuator (`info`,
 * `metrics`, `env`).
 *
 * The version is read from the JAR manifest's `Implementation-Version`
 * attribute, which Spring Boot's Gradle plugin populates automatically
 * for `bootJar` outputs. When running from an exploded classpath
 * (tests, IDE), the value is `unknown`.
 */
class CdcOutboxInfoContributor(
    private val properties: CdcOutboxProperties,
    private val sinkRegistry: ObjectProvider<EventSinkRegistry>,
    private val source: ObjectProvider<CdcSource>,
) : InfoContributor {

    override fun contribute(builder: Info.Builder) {
        val details = linkedMapOf<String, Any?>(
            "version" to libraryVersion(),
            "processor" to mapOf("kind" to properties.processor.kind.name),
            "replication" to mapOf(
                "slot" to properties.replication.slotName,
                "outputPlugin" to properties.replication.outputPlugin,
                "formatVersion" to properties.replication.formatVersion.name,
            ),
            "source" to mapOf(
                "type" to (source.ifAvailable?.javaClass?.simpleName ?: "none"),
            ),
            "sinks" to mapOf(
                "schemes" to (
                    sinkRegistry.ifAvailable?.knownSchemes()?.sorted()
                        ?: emptyList<String>()
                    ),
            ),
            "mappings" to mapOf("count" to properties.mappings.size),
            "checkpoint" to mapOf(
                "enabled" to properties.checkpoint.enabled,
                "directory" to properties.checkpoint.directory,
            ),
            "lag" to mapOf(
                "enabled" to properties.lag.enabled,
                "interval" to properties.lag.interval.toString(),
            ),
            "dlqReplay" to mapOf("enabled" to properties.dlq.replay.enabled),
            "replay" to mapOf("enabled" to properties.replay.enabled),
        )
        builder.withDetail("cdc-outbox", details)
    }

    /**
     * Returns the implementation version baked into this class' JAR
     * manifest, falling back to `unknown` on exploded classpaths
     * (tests, IDE runs).
     */
    private fun libraryVersion(): String =
        this::class.java.`package`?.implementationVersion ?: "unknown"
}
