package br.com.fltech.cdc.outbox.publisher.adapter.source.oracle

import br.com.fltech.cdc.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.cdc.outbox.publisher.core.port.CdcSource

/**
 * Placeholder [CdcSource] for Oracle Database.
 *
 * The real adapter (Wave 5) will choose between:
 *  - LogMiner (deprecated path, still works, requires SYS privileges),
 *  - OpenLogReplicator (open-source, modern), or
 *  - GoldenGate (enterprise tier).
 *
 * Calling `open`/`poll`/`ack` throws so a misconfigured deploy fails
 * loudly instead of silently producing nothing.
 */
class OracleCdcSourceStub : CdcSource {
    override fun open(): Nothing = unsupported("open")
    override fun poll(): OutboxEvent? = unsupported("poll")
    override fun ack(event: OutboxEvent): Nothing = unsupported("ack")
    override fun close(): Unit = Unit

    private fun unsupported(method: String): Nothing =
        throw UnsupportedOperationException(
            "OracleCdcSourceStub.$method() is not implemented (Wave 5 deliverable). " +
                "Wire a real CdcSource bean to enable Oracle support.",
        )
}
