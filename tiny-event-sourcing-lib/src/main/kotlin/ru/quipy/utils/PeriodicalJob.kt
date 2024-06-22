package ru.quipy.utils

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.quipy.utils.Delayer.*
import ru.quipy.utils.PeriodicalJob.MaintenanceJobState.STOPPED
import java.util.concurrent.ExecutorService
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.system.measureTimeMillis
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

class PeriodicalJob(
    val jobNamespace: String,
    val name: String,
    private val executor: ExecutorService,
    maintenanceDispatcher: CoroutineDispatcher =
        executor.asCoroutineDispatcher(),
    maintenanceExceptionHandler: CoroutineExceptionHandler = createDefaultExceptionHandler("$jobNamespace:$name"),
    private val relaunchOnUnexpectedCompletion: Boolean = true,
    private val delayBeforeFirstExecutionMillis: Long = 0,
    private val delayer: Delayer,
    private val timeout: Duration = 1.hours,
    coroutineContext: CoroutineContext = EmptyCoroutineContext, // some elements might be overwritten
    private val maintenanceFunction: suspend (invocationNum: Int, job: PeriodicalJob) -> Unit
) {

    companion object {
        private val logger = LoggerFactory.getLogger(PeriodicalJob::class.java)

        fun createDefaultExceptionHandler(jobName: String) =
            CoroutineExceptionHandler { _, th -> logger.error("Exception in periodical job $jobName", th) }
    }

    private val maintenanceScope =
        CoroutineScope(coroutineContext.plus(SupervisorJob() + maintenanceDispatcher + maintenanceExceptionHandler))

    private val maintenanceCompletionHandler: CompletionHandler = { th: Throwable? ->
        if (jobState != STOPPED && relaunchOnUnexpectedCompletion) {
            maintenanceJob = launchMaintenanceJob()
        }
        maintenanceJob.isActive
    }

    @Volatile
    var jobState = MaintenanceJobState.ACTIVE
        private set

    @Volatile
    private var maintenanceJob = launchMaintenanceJob()

    private fun launchMaintenanceJob() =
        maintenanceScope.launch {
            delay(delayBeforeFirstExecutionMillis)
            var invocationNum = 0
            while (jobState != STOPPED) {
                val maintenanceDuration = measureTimeMillis {
                    try {
                        withTimeout(timeout.inWholeMilliseconds) {
                            maintenanceFunction(invocationNum, this@PeriodicalJob)
                        }
                    } catch (ignored: TimeoutCancellationException) {
                    }
                }
                invocationNum++

                when (delayer) {
                    is FixedDelay -> delayer.delay()
                    is StartTimePreservingDelay -> delayer.delay()
                    is InvocationRatePreservingDelay -> delayer.delay(maintenanceDuration)
                }
            }
        }.also {
            it.invokeOnCompletion(maintenanceCompletionHandler)
        }

    fun stop() {
        jobState = STOPPED
        maintenanceJob.cancel()
    }

    enum class MaintenanceJobState {
        ACTIVE,
        STOPPED,
    }
}

sealed class Delayer(
    val delayDuration: Duration
) {
    class FixedDelay(
        duration: Duration
    ) : Delayer(duration) {
        suspend fun delay() {
            delay(delayDuration.inWholeMilliseconds)
        }
    }

    class InvocationRatePreservingDelay(
        duration: Duration
    ) : Delayer(duration) {
        suspend fun delay(alreadySpentTimeMillis: Long) {
            val desiredDelay = delayDuration.inWholeMilliseconds
            kotlinx.coroutines.delay(
                if (alreadySpentTimeMillis >= desiredDelay)
                    0
                else
                    desiredDelay - alreadySpentTimeMillis
            )
        }
    }

    class StartTimePreservingDelay(
        duration: Duration
    ) : Delayer(duration) {
        suspend fun delay() {
            val delayMillis = delayDuration.inWholeMilliseconds
            delay(delayMillis - (System.currentTimeMillis() % delayMillis))
        }
    }
}