package ru.quipy.streams

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.database.EventStore
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

interface EventReader {
    suspend fun read(batchSize: Int): List<EventRecord>
    fun commitReadIndex(index: Long)

    /**
     * We can "replay" events in the stream by resetting it to desired reading index
     */
    fun resetReadIndex(resetInfo: ReadIndexResetInfo)
    fun stop()
}

class EventStoreReader(
    private val eventStore: EventStore,
    private val eventsChannel: EventsChannel,
    private val streamName: String,
    private val tableName: String,
    private val retryConfig: RetryConf,
    private val eventStreamNotifier: EventStreamNotifier,
    private val dispatcher: CoroutineDispatcher
) : EventReader {
    companion object {
        private val NO_RESET_REQUIRED = ReadIndexResetInfo(-1)
    }

    private val logger: Logger = LoggerFactory.getLogger(EventStoreReader::class.java)
    private val nextReaderAliveCheck: Duration = 15.seconds

    private val streamManager: EventStreamReaderManager = ActiveEventStreamReaderManager(eventStore)
    private val isReaderPrimary: AtomicBoolean = AtomicBoolean(false)
    private val isScanActive: Boolean = true

    private val scanJob: Job = launchEventStoreReaderScanJob()

    private var streamReadIndex: EventStreamReadIndex = EventStreamReadIndex(streamName, readIndex = 0L, version = 0L)
    private var indexResetInfo: ReadIndexResetInfo = NO_RESET_REQUIRED
    private var processedRecords = 0L

    override suspend fun read(batchSize: Int): List<EventRecord> {
        if (!isReaderPrimary.get())
            return emptyList()

        checkAndResetIndexIfRequired()

        val eventsBatch = eventStore.findBatchOfEventRecordAfter(tableName, streamReadIndex.readIndex, batchSize)

        eventStreamNotifier.onBatchRead(streamName, eventsBatch.size)

        var processingRecordTimestamp: Long

        eventsBatch.forEach { eventRecord ->
            processingRecordTimestamp = eventRecord.createdAt

            feedToHandling(streamReadIndex.readIndex, eventRecord) {
                eventStreamNotifier.onRecordHandledSuccessfully(streamName, eventRecord.eventTitle)

                if (processedRecords++ % 10 == 0L)
                    commitReadIndex(processingRecordTimestamp)
            }
        }

        return eventsBatch
    }

    override fun commitReadIndex(index: Long) {
        EventStreamReadIndex(streamName, index, streamReadIndex.version + 1L).also {
            logger.info("Committing index for $streamName, index: ${index}, current version: ${streamReadIndex.version}")
            eventStore.commitStreamReadIndex(it)
            eventStreamNotifier.onReadIndexCommitted(streamName, it.readIndex)
        }

        syncReaderIndex()
    }

    override fun resetReadIndex(resetInfo: ReadIndexResetInfo) {
        if (streamReadIndex.version < 1)
            throw IllegalArgumentException("Can't reset to non existing version: ${streamReadIndex.version}")

        indexResetInfo = resetInfo
    }

    override fun stop() {
        if (isReaderPrimary.get())
            isReaderPrimary.set(false)

        if (isScanActive)
            scanJob.cancel()
    }

    private fun launchEventStoreReaderScanJob(): Job {
        return CoroutineScope(CoroutineName("reading-$streamName-coroutine") + dispatcher).launch {
            while (isScanActive) {
                if (streamManager.isReaderAlive(streamName)) {
                    logger.debug("Reader of stream $streamName is alive. Waiting $nextReaderAliveCheck before continuing...")
                    delay(nextReaderAliveCheck.inWholeMilliseconds)
                } else if (streamManager.tryInterceptReading(streamName)) {3
                    ensureTableExists()
                    syncReaderIndex()
                    isReaderPrimary.set(true)
                } else {
                    logger.info("Failed to intercept reading of stream $streamName, because someone else succeeded first.")
                    continue
                }
            }
        }
    }

    private suspend fun ensureTableExists() {
        while (!eventStore.tableExists(tableName)) {
            delay(2_000)
            logger.trace("Event stream $streamName is waiting for $tableName to be created")
        }
    }

    private suspend fun feedToHandling(readingIndex: Long, event: EventRecord, beforeNextPerform: () -> Unit) {
        for (attemptNum in 1..retryConfig.maxAttempts) {
            eventsChannel.sendEvent(EventsChannel.EventRecordForHandling(readingIndex, event))
            if (eventsChannel.receiveConfirmation()) {
                beforeNextPerform()
                return
            }

            if (attemptNum == retryConfig.maxAttempts) {
                when (retryConfig.lastAttemptFailedStrategy) {
                    RetryFailedStrategy.SKIP_EVENT -> {
                        logger.error("Event stream: $streamName. Retry attempts failed $attemptNum times. SKIPPING...")
                        beforeNextPerform()
                        return
                    }
                    RetryFailedStrategy.SUSPEND -> {
                        logger.error("Event stream: $streamName. Retry attempts failed $attemptNum times. SUSPENDING THE HOLE STREAM...")
                        eventStreamNotifier.onRecordSkipped(streamName, event.eventTitle, attemptNum)
                        delay(Long.MAX_VALUE) // todo sukhoa find the way better
                    }
                }
            }
            eventStreamNotifier.onRecordHandlingRetry(streamName, event.eventTitle, attemptNum)
        }
    }

    private fun syncReaderIndex() {
        eventStore.findStreamReadIndex(streamName)?.also {
            streamReadIndex = it
            logger.info("Reader index synced for $streamName. Index: ${it.readIndex}, version: ${it.version}")
            eventStreamNotifier.onReadIndexSynced(streamName, it.readIndex)
        }
    }

    private fun checkAndResetIndexIfRequired() {
        if (indexResetInfo != NO_RESET_REQUIRED) {
            val updatedReadIndex = EventStreamReadIndex(streamName, indexResetInfo.resetIndex, streamReadIndex.version)

            streamReadIndex = updatedReadIndex
            logger.warn("Index for stream $streamName forcibly reset to ${indexResetInfo.resetIndex}")

            indexResetInfo = NO_RESET_REQUIRED
            eventStreamNotifier.onStreamReset(streamName, indexResetInfo.resetIndex)
        }
    }
}

class ReadIndexResetInfo(
    val resetIndex: Long
)