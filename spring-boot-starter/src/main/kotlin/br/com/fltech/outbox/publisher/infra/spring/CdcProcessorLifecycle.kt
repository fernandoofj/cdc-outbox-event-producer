package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.core.application.CdcProcessor
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * SmartLifecycle wrapper for [CdcProcessor] — analogous to
 * [CdcOutboxLifecycle] but for the hexagonal orchestrator.
 *
 * Identical lifecycle semantics: daemon thread, cooperative shutdown
 * with a finite timeout, same phase value so HTTP + Actuator are up
 * across both edges.
 */
class CdcProcessorLifecycle(
    private val processor: CdcProcessor,
) : SmartLifecycle {

    @Volatile
    private var executor: ExecutorService? = null

    @Volatile
    private var running = false

    override fun start() {
        if (running) {
            logger.debug("CdcProcessorLifecycle already running; ignoring start()")
            return
        }
        logger.info("Starting CDC outbox hexagonal processor")
        // ThreadFactory wires an UncaughtExceptionHandler so any
        // Error (OOM, StackOverflow) is logged + signals
        // `running=false` for the health indicator — same operator
        // visibility as the in-method catch, without swallowing
        // Error inside the try.
        val pool = Executors.newSingleThreadExecutor { r ->
            Thread(r, THREAD_NAME).apply {
                isDaemon = true
                uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, t ->
                    logger.error("CDC outbox processor died with unrecoverable error", t)
                    running = false
                }
            }
        }
        executor = pool
        running = true
        pool.execute {
            try {
                processor.start()
            } catch (e: Exception) {
                logger.error("CDC outbox processor terminated unexpectedly", e)
            } finally {
                running = false
            }
        }
    }

    override fun stop() {
        if (!running && executor == null) return
        logger.info("Stopping CDC outbox hexagonal processor")
        processor.stop()
        executor?.let { pool ->
            pool.shutdown()
            val terminated = try {
                pool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                logger.warn("Interrupted while awaiting CDC outbox processor shutdown", e)
                false
            }
            if (!terminated) {
                logger.warn(
                    "CDC outbox processor did not terminate within {} s; sending interrupt",
                    SHUTDOWN_TIMEOUT_SECONDS,
                )
                pool.shutdownNow()
            }
        }
        executor = null
        running = false
    }

    override fun isRunning(): Boolean = running
    override fun isAutoStartup(): Boolean = true
    override fun getPhase(): Int = CdcOutboxLifecycle.LIFECYCLE_PHASE

    companion object {
        private val logger = LoggerFactory.getLogger(CdcProcessorLifecycle::class.java)
        private const val THREAD_NAME = "cdc-outbox-processor"
        private const val SHUTDOWN_TIMEOUT_SECONDS = 30L
    }
}
