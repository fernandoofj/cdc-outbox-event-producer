package br.com.fltech.outbox.publisher.adapter.source.sqlserver

import br.com.fltech.outbox.publisher.core.domain.OutboxEvent
import br.com.fltech.outbox.publisher.core.port.CdcSource

/**
 * Placeholder [CdcSource] for SQL Server.
 *
 * The real adapter will be implemented in Wave 5 and use either:
 *  - SQL Server Change Tracking (cheap, only "row X changed"), or
 *  - SQL Server Change Data Capture (richer, per-column history).
 *
 * Instantiating this class is permitted so the [CdcSource]-typed beans
 * compile; any call to `open` / `poll` / `ack` throws so deploys fail
 * loudly if a misconfiguration ever points at this stub.
 */
class SqlServerCdcSourceStub : CdcSource {
    override fun open(): Nothing = unsupported("open")

    override fun poll(): OutboxEvent? = unsupported("poll")

    override fun ack(event: OutboxEvent): Nothing = unsupported("ack")

    override fun close(): Unit = Unit

    private fun unsupported(method: String): Nothing =
        throw UnsupportedOperationException(
            "SqlServerCdcSourceStub.$method() is not implemented (Wave 5 deliverable). " +
                "Wire a real CdcSource bean to enable SQL Server support.",
        )
}
