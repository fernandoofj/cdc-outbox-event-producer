package br.com.fltech.outbox.publisher.core.port

import java.util.concurrent.ConcurrentHashMap

/**
 * In-memory [CheckpointStore] for tests. Lives in `src/test/` so the
 * production module never accidentally depends on it. Thread-safe so
 * tests that spawn the orchestrator on a daemon thread can read back
 * the latest persisted value without a fence.
 */
class InMemoryCheckpointStore(
    initial: Map<String, String> = emptyMap(),
) : CheckpointStore {

    private val store = ConcurrentHashMap<String, String>().apply { putAll(initial) }

    /** Total `save` calls observed — handy for asserting per-ack persistence. */
    @Volatile
    var saveCount: Int = 0
        private set

    /** Total `load` calls observed. */
    @Volatile
    var loadCount: Int = 0
        private set

    override fun load(key: String): String? {
        loadCount += 1
        return store[key]
    }

    override fun save(key: String, value: String) {
        saveCount += 1
        store[key] = value
    }

    fun snapshot(): Map<String, String> = store.toMap()
}
