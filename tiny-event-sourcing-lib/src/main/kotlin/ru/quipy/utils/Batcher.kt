package ru.quipy.utils

import org.slf4j.LoggerFactory
import ru.quipy.core.exceptions.DuplicateEventIdException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class Batcher(
    private val batchCommandSize: Int,
    private val batchWindow: Duration,
    private val batchUpdater: (List<String>) -> Array<Int>
) {
    private val lock = ReentrantLock()

    private val collectingBatch = mutableListOf<Command>()
    private val idSet = mutableSetOf<String>()
    private var collectingStartedAt = System.currentTimeMillis()

    private val batchExecJob = PeriodicalJob(
        "event-store",
        "batch-execute",
        Executors.newSingleThreadExecutor(NamedThreadFactory("batcher-executor")),
        delayer = Delayer.InvocationRatePreservingDelay(2.milliseconds),
    ) { _, _ ->
        if (collectingBatch.size >= batchCommandSize || System.currentTimeMillis() - collectingStartedAt >= batchWindow.inWholeMilliseconds) {
            executeBatch()
        }
    }

    fun delayedExecution(id: String, statement: String): CompletableFuture<Boolean> {
        val command = Command(statement, CompletableFuture())
        lock.withLock {
            if (!idSet.add(id)) {
                throw DuplicateEventIdException(
                    "There is record with such an id in batch. Record cannot be saved $id",
                    null
                )
            }
            collectingBatch.add(command)
        }
        return command.completableFuture
    }

    private fun executeBatch() {
        var copyCommands: List<Command>
        lock.withLock {
            copyCommands = collectingBatch.toList()
            if (collectingBatch.isNotEmpty()) {
                collectingBatch.clear()
                collectingStartedAt = System.currentTimeMillis()
                idSet.clear()
            }

            if (copyCommands.isEmpty()) return

            try {
                val errored = batchUpdater(copyCommands.map { it.statement }.toList()).toSet()
                copyCommands.forEachIndexed { i, c ->
                    c.completableFuture.complete(!errored.contains(i))
                }
            } catch (e: Exception) {
                val batchStatement = copyCommands.joinToString(";") { it.statement }
                logger.error("Batch execution failed Statement: $batchStatement", e)
                copyCommands.forEach { it.completableFuture.completeExceptionally(e) }
            }
        }
    }

    class Command(
        val statement: String,
        val completableFuture: CompletableFuture<Boolean>
    )

    companion object {
        private val logger = LoggerFactory.getLogger(Batcher::class.java)
    }
}