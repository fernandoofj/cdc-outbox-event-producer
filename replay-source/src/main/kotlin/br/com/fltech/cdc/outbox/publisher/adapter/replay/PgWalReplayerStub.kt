package br.com.fltech.cdc.outbox.publisher.adapter.replay

import br.com.fltech.cdc.outbox.publisher.core.port.RowChangeSource
import br.com.fltech.cdc.outbox.publisher.core.port.SourceReplayer
import br.com.fltech.cdc.outbox.publisher.core.port.UnsupportedReplayException

/**
 * Placeholder for the Postgres WAL replayer. Postgres logical
 * replication slots are monotonic — there is no built-in way to
 * "rewind" the slot to a past LSN. A real implementation needs
 * either:
 *
 *  - a temporary replication slot created at boot from an archived
 *    WAL segment, OR
 *  - `pg_logical_slot_peek_changes(...)` which is bounded to the
 *    slot's current retention window.
 *
 * Both paths require operational decisions (WAL retention,
 * archive_command, slot lifecycle) that vary per deployment. This
 * stub throws [UnsupportedReplayException] so a replay request
 * targeted at Postgres fails fast and surfaces the limitation.
 */
class PgWalReplayerStub : SourceReplayer {

    override val sourceKind: String = SOURCE_KIND

    override fun openBoundedSource(fromPosition: String, toPosition: String): RowChangeSource {
        throw UnsupportedReplayException(
            "Postgres WAL replay is not implemented yet. Postgres logical replication slots are " +
                "monotonic — replaying a past LSN window requires either a temporary slot created " +
                "from an archived WAL segment OR `pg_logical_slot_peek_changes` against the live " +
                "slot bounded by its retention. Open a tracking issue if you need this.",
        )
    }

    companion object {
        const val SOURCE_KIND = "postgres-wal"
    }
}
