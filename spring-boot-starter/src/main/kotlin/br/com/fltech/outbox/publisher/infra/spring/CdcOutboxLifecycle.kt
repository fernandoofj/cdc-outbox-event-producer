package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.workflow.SlotReaderMessageProducer
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Bridges the [SlotReaderMessageProducer] streaming loop into the Spring
 * application lifecycle.
 *
 *  - On context refresh: starts a dedicated daemon thread that runs the
 *    blocking `startStreaming()` loop.
 *  - On context close: signals `stopStreaming()`, interrupts the worker if
 *    necessary, and waits up to [SHUTDOWN_TIMEOUT_SECONDS] for graceful exit.
 *
 * `autoStartup = true` matches typical Boot apps. `phase = Int.MAX_VALUE - 1`
 * starts late and stops early so DataSources, brokers, and Actuator
 * endpoints are alive on both ends.
 */
class CdcOutboxLifecycle(
    private val producer: SlotReaderMessageProducer,
) : SmartLifecycle {
    @Volatile
    private var executor: ExecutorService? = null

    @Volatile
    private var running = false

    override fun start() {
        if (running) {
            logger.debug("CdcOutboxLifecycle already running; ignoring start()")
            return
        }
        logger.info("Starting CDC outbox producer")
        // ThreadFactory installs an UncaughtExceptionHandler so any
        // Error (OOM, StackOverflow) is logged + `running` cleared
        // for the health indicator. Exception is caught in-method
        // and logged at ERROR with `running=false` in `finally`.
        val pool =
            Executors.newSingleThreadExecutor { r ->
                Thread(r, THREAD_NAME).apply {
                    isDaemon = true
                    uncaughtExceptionHandler =
                        Thread.UncaughtExceptionHandler { _, t ->
                            logger.error("CDC outbox producer died with unrecoverable error", t)
                            running = false
                        }
                }
            }
        executor = pool
        running = true
        pool.execute {
            try {
                producer.startStreaming()
            } catch (e: Exception) {
                logger.error("CDC outbox producer terminated unexpectedly", e)
            } finally {
                running = false
            }
        }
    }

    override fun stop() {
        if (!running && executor == null) return
        logger.info("Stopping CDC outbox producer")
        producer.stopStreaming()
        executor?.let { pool ->
            pool.shutdown()
            val terminated =
                try {
                    pool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    logger.warn("Interrupted while awaiting CDC outbox shutdown", e)
                    false
                }
            if (!terminated) {
                logger.warn(
                    "CDC outbox loop did not terminate within {} s; sending interrupt",
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

    override fun getPhase(): Int = LIFECYCLE_PHASE

    companion object {
        private val logger = LoggerFactory.getLogger(CdcOutboxLifecycle::class.java)
        private const val THREAD_NAME = "cdc-outbox-slot-reader"
        private const val SHUTDOWN_TIMEOUT_SECONDS = 30L

        /**
         * Spring Boot 3.2+ uses `Int.MAX_VALUE - 1` for
         * `WebServerStartStopLifecycle` and `Int.MAX_VALUE - 2048` for
         * `WebServerGracefulShutdownLifecycle`. We start AFTER both so the
         * HTTP server and Actuator are reachable when the streaming loop
         * begins, and we stop BEFORE both so the HTTP probes can still
         * report `DOWN` while the producer is draining. `MAX_VALUE - 4096`
         * sits cleanly below the web-server pair without colliding with
         * any well-known Boot phase.
         */
        const val LIFECYCLE_PHASE = Int.MAX_VALUE - 4096
    }
}
