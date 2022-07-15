package ru.quipy.streams

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import org.slf4j.LoggerFactory
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.Aggregate
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy.SKIP_EVENT
import ru.quipy.streams.annotation.RetryFailedStrategy.SUSPEND
import java.util.concurrent.atomic.AtomicBoolean


class BufferedAggregateEventStream<A : Aggregate>(
    override val streamName: String,
    private val streamReadPeriod: Long, // todo sukhoa wrong naming
    private val streamBatchSize: Int,
    private val tableName: String,
    private val retryConfig: RetryConf,
    private val eventStoreDbOperations: EventStoreDbOperations,
    private val eventStreamNotifier: EventStreamNotifier,
    private val dispatcher: CoroutineDispatcher
) : AggregateEventStream<A> {
    companion object {
        private val logger = LoggerFactory.getLogger(BufferedAggregateEventStream::class.java)
        private val NO_RESET_REQUIRED = ResetInfo(-1)
    }

    private val eventsChannel: Channel<EventRecordForHandling> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    private val acknowledgesChannel: Channel<EventConsumedAck> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    @Volatile
    private var resetInfo = NO_RESET_REQUIRED

    private var active = AtomicBoolean(true)

    private var suspended = AtomicBoolean(false)

    @Volatile
    override var readingIndex = 0L
        private set

    @Volatile
    private var readerIndexCommittedVersion = 0L

    @Volatile
    private var processedRecords = 0L

    private val eventStreamCompletionHandler: CompletionHandler = { th: Throwable? ->
        if (active.get()) {
            logger.error(
                "Unexpected error in aggregate event stream ${streamName}. Index $readingIndex. Relaunching",
                th
            )
            eventStreamJob = launchEventStream()
        } else {
            logger.warn("Stopped event stream coroutine. Stream: $streamName, index $readingIndex")
        }
    }

    @Volatile
    private var eventStreamJob = launchEventStream()

    private fun launchEventStream() =
        CoroutineScope(CoroutineName("reading-$streamName-coroutine") + dispatcher).launch {
            // initial delay
            delay(5_000)
            eventStreamNotifier.onStreamLaunched(streamName)

            suspendTillTableExists()
            syncReaderIndex() // todo sukhoa we don't use read index after relaunching

            while (active.get()) {
                while (suspended.get()) {
                    delay(5_000)
                    logger.info("Suspended stream $streamName with reading index $readingIndex")
                }

                checkAndResetIfRequired()

                val eventsBatch =
                    eventStoreDbOperations.findBatchOfEventRecordAfter(tableName, readingIndex, streamBatchSize)
                eventStreamNotifier.onBatchRead(streamName, eventsBatch.size)

                if (eventsBatch.isEmpty()) {
                    delay(streamReadPeriod)
                }

                var processingRecordTimestamp: Long
                eventsBatch.forEach { eventRecord ->
                    processingRecordTimestamp = eventRecord.createdAt

                    feedToHandling(readingIndex, eventRecord) {
                        eventStreamNotifier.onRecordHandledSuccessfully(streamName, eventRecord.eventTitle)
                        readingIndex = processingRecordTimestamp

                        if (processedRecords++ % 10 == 0L) {
                            commitReaderIndexAndSync()
                        }
                    }
                }
            }
        }.also {
            it.invokeOnCompletion(eventStreamCompletionHandler)
        }

    private suspend fun feedToHandling(readingIndex: Long, event: EventRecord, beforeNextPerform: () -> Unit) {
        for (attemptNum in 1..retryConfig.maxAttempts) {
            eventsChannel.send(EventRecordForHandling(readingIndex, event))
            if (acknowledgesChannel.receive().successful) {
                beforeNextPerform()
                return
            }

            if (attemptNum == retryConfig.maxAttempts) {
                when (retryConfig.lastAttemptFailedStrategy) {
                    SKIP_EVENT -> {
                        logger.error("Event stream: $streamName. Retry attempts failed $attemptNum times. SKIPPING...")
                        beforeNextPerform()
                        return
                    }
                    SUSPEND -> {
                        logger.error("Event stream: $streamName. Retry attempts failed $attemptNum times. SUSPENDING THE HOLE STREAM...")
                        eventStreamNotifier.onRecordSkipped(streamName, event.eventTitle, attemptNum)
                        delay(Long.MAX_VALUE) // todo sukhoa find the way better
                    }
                }
            }
            eventStreamNotifier.onRecordHandlingRetry(streamName, event.eventTitle, attemptNum)
        }
    }

    private suspend fun suspendTillTableExists() {
        while (!eventStoreDbOperations.tableExists(tableName)) {
            delay(2_000)
            logger.trace("Event stream $streamName is waiting for $tableName to be created")
        }
    }

    private fun syncReaderIndex() {
        eventStoreDbOperations.findStreamReadIndex(streamName)?.also {
            readingIndex = it.readIndex
            readerIndexCommittedVersion = it.version
            logger.info("Reader index synced for $streamName. Index: $readingIndex, version: $readerIndexCommittedVersion")
            eventStreamNotifier.onReadIndexSynced(streamName, readingIndex)
        }
    }

    private fun commitReaderIndexAndSync() {
        EventStreamReadIndex(streamName, readingIndex, readerIndexCommittedVersion + 1L).also {
            logger.info("Committing index for $streamName, index: $readingIndex, current version: $readerIndexCommittedVersion")
            eventStoreDbOperations.commitStreamReadIndex(it)
            eventStreamNotifier.onReadIndexCommitted(streamName, it.readIndex)
        }
        syncReaderIndex()
    }

    private fun checkAndResetIfRequired() {
        if (resetInfo != NO_RESET_REQUIRED) {
            readingIndex = resetInfo.resetIndex
            logger.warn("Index for stream $streamName forcibly reset to $readingIndex")
            resetInfo = NO_RESET_REQUIRED
            eventStreamNotifier.onStreamReset(streamName, readingIndex)
        }
    }

    override suspend fun handleNextRecord(eventProcessingFunction: suspend (EventRecord) -> Boolean) {
        val receive = eventsChannel.receive()
        try {
            eventProcessingFunction(receive.record).also {
                if (!it) logger.info("Processing function return false for event record: ${receive.record} at index: ${receive.readIndex}`")
                acknowledgesChannel.send(EventConsumedAck(receive.readIndex, it))
            }
        } catch (e: Exception) {
            logger.error(
                "Error while invoking event handling function at index: ${receive.readIndex} event record: ${receive.record}",
                e
            )
            acknowledgesChannel.send(EventConsumedAck(receive.readIndex, false))
        }
    }

    override fun stopAndDestroy() {
        if (!active.compareAndSet(true, false)) return
        // todo sukhoa think of committing last read index

        if (eventStreamJob.isActive) {
            eventStreamJob.cancel()
        }
    }

    override fun suspend() {
        suspended.set(true)
    }

    override fun resume() {
        logger.info("Resuming stream $streamName with reading index $readingIndex")
        suspended.set(false)
    }

    override fun resetToReadingIndex(version: Long) {
        if (version < 1) throw IllegalArgumentException("Can't reset to non existing version: $version")
        resetInfo = ResetInfo(version)
    }

    class EventConsumedAck(
        val readIndex: Long,
        val successful: Boolean,
    )

    class EventRecordForHandling(
        val readIndex: Long,
        val record: EventRecord,
    )

    class ResetInfo(
        val resetIndex: Long
    )
}