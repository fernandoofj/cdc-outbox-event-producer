package br.com.fltech.cdc.outbox.publisher.core.port

import br.com.fltech.cdc.outbox.publisher.core.domain.RowChange

/**
 * Lower-level driving port for adapters that surface *row-level* CDC
 * (Postgres `wal2json` I/U/D records, MySQL binlog row events, polled
 * outbox tables that carry full column data).
 *
 * Pairs with [MappingRules] and the
 * [br.com.fltech.cdc.outbox.publisher.core.application.MappingCdcSource]
 * decorator to satisfy the higher-level [CdcSource] used by the
 * orchestrator.
 *
 * Lifecycle and threading match [CdcSource] — see its KDoc for the
 * single-threaded contract.
 */
interface RowChangeSource : AutoCloseable {
    fun open()
    fun poll(): RowChange?
    fun ack(rowChange: RowChange)
    override fun close()
}
