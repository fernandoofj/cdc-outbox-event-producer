package br.com.fltech.outbox.publisher.adapter.checkpoint

import br.com.fltech.outbox.publisher.observability.CdcOutboxMetrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Drives the file-backed checkpoint store across the contract
 * surfaces a deployed service relies on:
 *  - round-trip per key, with values mixing colons / slashes that
 *    appear in real LSN / binlog strings,
 *  - missing key returns `null` (so the source falls back to its
 *    natural start position),
 *  - overwrite leaves no stale `.tmp` artefact,
 *  - corrupt file returns `null` and is logged (the file is left
 *    untouched for operator inspection),
 *  - keys with filesystem-hostile characters (`:`) round-trip via
 *    sanitisation, and a sanitised collision is detected and
 *    rejected.
 */
class FileCheckpointStoreTest {

    @Test
    fun `save then load round-trips for a typical binlog checkpoint`(@TempDir dir: Path) {
        val store = FileCheckpointStore(dir)
        store.save("binlog:65536", "mysql-bin.000004:891")
        assertEquals("mysql-bin.000004:891", store.load("binlog:65536"))
    }

    @Test
    fun `save then load round-trips for a Postgres LSN`(@TempDir dir: Path) {
        val store = FileCheckpointStore(dir)
        store.save("pg-wal:orders_outbox_slot", "0/16E8198")
        assertEquals("0/16E8198", store.load("pg-wal:orders_outbox_slot"))
    }

    @Test
    fun `load returns null for an unknown key`(@TempDir dir: Path) {
        val store = FileCheckpointStore(dir)
        assertNull(store.load("never-saved"))
    }

    @Test
    fun `save twice keeps only the latest value and leaves no orphan tmp file`(@TempDir dir: Path) {
        val store = FileCheckpointStore(dir)
        store.save("binlog:1", "mysql-bin.000001:120")
        store.save("binlog:1", "mysql-bin.000001:240")
        assertEquals("mysql-bin.000001:240", store.load("binlog:1"))

        val orphans = Files.list(dir).use { it.toList().filter { p -> p.fileName.toString().endsWith(".tmp") } }
        assertTrue(orphans.isEmpty(), "Unexpected tmp leftovers: $orphans")
    }

    @Test
    fun `corrupt checkpoint file reads as null without throwing`(@TempDir dir: Path) {
        // Simulate a half-written file by writing raw bytes that don't
        // decode as JSON. The store must treat this as "missing" so
        // the source can fall back to its natural start position
        // rather than refusing to come up.
        val store = FileCheckpointStore(dir)
        val key = "binlog:777"
        // First a clean save to learn the on-disk filename, then
        // overwrite it with garbage.
        store.save(key, "mysql-bin.000099:1")
        val saved = Files.list(dir).use { it.toList().single { p -> !p.fileName.toString().endsWith(".tmp") } }
        Files.writeString(saved, "this is not json")

        assertNull(store.load(key))
        // The corrupt file is intentionally left in place.
        assertTrue(Files.exists(saved))
    }

    @Test
    fun `value field present but key field mismatch reads as null`(@TempDir dir: Path) {
        // Two distinct keys can sanitise to the same on-disk filename.
        // The store's defence is the `key` field stored inside the
        // JSON — load returns null when it doesn't match the caller's
        // expectation.
        val store = FileCheckpointStore(dir)
        store.save("binlog:1", "a")
        // Manually rewrite the file with a different key while keeping
        // the on-disk name intact.
        val saved = Files.list(dir).use { it.toList().single() }
        Files.writeString(saved, """{"key":"binlog:2","value":"a"}""")

        assertNull(store.load("binlog:1"))
    }

    @Test
    fun `directory is created lazily so first save does not need explicit mkdir`(@TempDir dir: Path) {
        val nested = dir.resolve("subdir").resolve("checkpoints")
        val store = FileCheckpointStore(nested)
        store.save("k", "v")
        assertEquals("v", store.load("k"))
        assertTrue(Files.isDirectory(nested))
    }

    @Test
    fun `value with quotes and backslashes survives the round trip`(@TempDir dir: Path) {
        // Defensive — real LSN strings don't contain these, but we
        // shouldn't corrupt the file if the contract changes.
        val store = FileCheckpointStore(dir)
        val payload = """weird "value" with \backslash"""
        store.save("k", payload)
        assertEquals(payload, store.load("k"))
    }

    @Test
    fun `orphan sweep deletes a single tmp file on construction`(@TempDir dir: Path) {
        // Simulate a crash mid-save: a `.tmp` file is on disk with no
        // canonical sibling. The constructor must reclaim it.
        val orphan = dir.resolve("binlog_1.json.tmp")
        Files.writeString(orphan, """{"key":"binlog:1","value":"abandoned"}""")

        val registry = SimpleMeterRegistry()
        FileCheckpointStore(dir, CdcOutboxMetrics(registry))

        assertFalse(Files.exists(orphan), "Orphan tmp file should have been swept")
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.CHECKPOINT_ORPHANS_SWEPT,
                CdcOutboxMetrics.TAG_OUTCOME, "deleted",
            ).count(),
        )
    }

    @Test
    fun `orphan sweep deletes every tmp file in one pass`(@TempDir dir: Path) {
        // Two orphans from independent keys must both be reclaimed.
        Files.writeString(dir.resolve("binlog_1.json.tmp"), "stale-a")
        Files.writeString(dir.resolve("pg-wal_orders.json.tmp"), "stale-b")
        Files.writeString(dir.resolve("noise.tmp"), "stale-c")

        val registry = SimpleMeterRegistry()
        FileCheckpointStore(dir, CdcOutboxMetrics(registry))

        val remaining = Files.list(dir).use { it.toList() }
        assertTrue(remaining.isEmpty(), "Sweep should have removed every .tmp; left: $remaining")
        assertEquals(
            3.0,
            registry.counter(
                CdcOutboxMetrics.CHECKPOINT_ORPHANS_SWEPT,
                CdcOutboxMetrics.TAG_OUTCOME, "deleted",
            ).count(),
        )
    }

    @Test
    fun `orphan sweep leaves canonical json and ad-hoc files alone`(@TempDir dir: Path) {
        // Canonical files and unrelated content (e.g. an operator's
        // README, a backup) must NEVER be touched — only `.tmp`.
        val canonical = dir.resolve("binlog_1.json")
        val adhoc = dir.resolve("notes.txt")
        val orphan = dir.resolve("binlog_1.json.tmp")
        Files.writeString(canonical, """{"key":"binlog:1","value":"durable"}""")
        Files.writeString(adhoc, "operator note")
        Files.writeString(orphan, "stale")

        val registry = SimpleMeterRegistry()
        FileCheckpointStore(dir, CdcOutboxMetrics(registry))

        assertTrue(Files.exists(canonical), "Canonical .json must survive the sweep")
        assertTrue(Files.exists(adhoc), "Ad-hoc non-.tmp must survive the sweep")
        assertFalse(Files.exists(orphan), "Orphan .tmp must be swept")
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.CHECKPOINT_ORPHANS_SWEPT,
                CdcOutboxMetrics.TAG_OUTCOME, "deleted",
            ).count(),
        )
    }

    @Test
    fun `orphan sweep is a no-op on an empty directory`(@TempDir dir: Path) {
        val registry = SimpleMeterRegistry()
        FileCheckpointStore(dir, CdcOutboxMetrics(registry))

        // No counter increments at all — the gauge effectively stays 0
        // because nothing was registered. Use `find` to be safe.
        val counter = registry.find(CdcOutboxMetrics.CHECKPOINT_ORPHANS_SWEPT).counter()
        assertEquals(0.0, counter?.count() ?: 0.0)
    }

    @Test
    fun `orphan sweep is a no-op when the parent directory does not yet exist`(@TempDir dir: Path) {
        // The store lazily creates the directory; the sweep then finds
        // an empty FS view. Either way: no crash, no counter bump.
        val nested = dir.resolve("not-yet-here").resolve("checkpoints")
        val registry = SimpleMeterRegistry()
        FileCheckpointStore(nested, CdcOutboxMetrics(registry))

        assertTrue(Files.isDirectory(nested))
        val counter = registry.find(CdcOutboxMetrics.CHECKPOINT_ORPHANS_SWEPT).counter()
        assertEquals(0.0, counter?.count() ?: 0.0)
    }

    @Test
    fun `orphan sweep is idempotent — second construction finds nothing to do`(@TempDir dir: Path) {
        Files.writeString(dir.resolve("binlog_1.json.tmp"), "stale")

        val registry = SimpleMeterRegistry()
        FileCheckpointStore(dir, CdcOutboxMetrics(registry))
        // Second construction over the same directory must be a clean
        // no-op — the sweep already reclaimed everything.
        FileCheckpointStore(dir, CdcOutboxMetrics(registry))

        // Total deleted-counter increments stays at 1; the second pass
        // had nothing to do.
        assertEquals(
            1.0,
            registry.counter(
                CdcOutboxMetrics.CHECKPOINT_ORPHANS_SWEPT,
                CdcOutboxMetrics.TAG_OUTCOME, "deleted",
            ).count(),
        )
    }

    @Test
    fun `orphan sweep records failed outcome and does not crash construction on IO error`(@TempDir dir: Path) {
        // Reproduce the "one bad file" path by dropping the parent
        // directory to read+execute only AFTER seeding `.tmp` orphans.
        // POSIX deletion authority lives on the parent inode, so each
        // `Files.deleteIfExists` will raise `AccessDeniedException`
        // (an IOException subclass). The sweep must keep going across
        // every file rather than aborting, and the constructor must
        // not throw.
        if (!supportsPosix(dir)) {
            // Windows / non-POSIX FS — skip rather than test a
            // wrong-platform pathway.
            return
        }
        Files.writeString(dir.resolve("binlog_1.json.tmp"), "a")
        Files.writeString(dir.resolve("binlog_2.json.tmp"), "b")
        val readonly = PosixFilePermissions.fromString("r-xr-xr-x")
        Files.setPosixFilePermissions(dir, readonly)

        val registry = SimpleMeterRegistry()
        try {
            FileCheckpointStore(dir, CdcOutboxMetrics(registry))
        } finally {
            // Restore write so JUnit's @TempDir cleanup can run.
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"))
        }

        // Every file attempted, every attempt recorded as failed —
        // proves the loop kept going past the first IO error.
        assertEquals(
            2.0,
            registry.counter(
                CdcOutboxMetrics.CHECKPOINT_ORPHANS_SWEPT,
                CdcOutboxMetrics.TAG_OUTCOME, "failed",
            ).count(),
        )
    }

    private fun supportsPosix(path: Path): Boolean =
        path.fileSystem.supportedFileAttributeViews().contains("posix")
}
