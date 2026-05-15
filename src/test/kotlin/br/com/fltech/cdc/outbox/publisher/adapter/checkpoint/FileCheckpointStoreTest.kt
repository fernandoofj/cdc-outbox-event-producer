package br.com.fltech.cdc.outbox.publisher.adapter.checkpoint

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
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
}
