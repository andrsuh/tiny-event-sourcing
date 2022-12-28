package ru.quipy.streams

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.database.EventStore
import ru.quipy.domain.Aggregate
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy.SKIP_EVENT
import ru.quipy.streams.annotation.RetryFailedStrategy.SUSPEND
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds


class BufferedAggregateEventStream<A : Aggregate>(
    override val streamName: String,
    private val streamReadPeriod: Long, // todo sukhoa wrong naming
    private val streamBatchSize: Int,
    private val tableName: String,
    private val retryConfig: RetryConf,
    private val eventStore: EventStore,
    private val eventStreamNotifier: EventStreamNotifier,
    private val dispatcher: CoroutineDispatcher
) : AggregateEventStream<A> {
    companion object {
        private val logger = LoggerFactory.getLogger(BufferedAggregateEventStream::class.java)
    }

    private val eventsChannel: Channel<EventRecordForHandling> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    private val acknowledgesChannel: Channel<EventConsumedAck> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    private var active = AtomicBoolean(true)

    private var suspended = AtomicBoolean(false)

    override val readingIndex: Long
        get() {
            return eventStoreReader.getReadIndex()
        }

    @Volatile
    private var processedRecords = 0L

    private val eventStreamCompletionHandler: CompletionHandler = { th: Throwable? ->
        if (active.get()) {
            logger.error(
                "Unexpected error in aggregate event stream ${streamName}. Relaunching...",
                th
            )
            eventStreamJob = launchJob()
        } else {
            logger.warn("Stopped event stream $streamName coroutine")
        }
    }

    @Volatile
    private var eventStreamJob: Job = launchJob()

    private val eventStoreReader: EventStoreReader = SingleEventStoreReader(eventStore, tableName, streamName, streamBatchSize, eventStreamNotifier, dispatcher)

    private fun launchJob() =
        CoroutineScope(CoroutineName("reading-$streamName-coroutine") + dispatcher).launch {
            // initial delay
            delay(5_000)
            eventStreamNotifier.onStreamLaunched(streamName)

            while (active.get()) {
                while (suspended.get()) {
                    logger.info("Suspending stream $streamName...")
                    delay(5_000)
                }

                val eventsBatch = eventStoreReader.read()

                if (eventsBatch.isEmpty()) {
                    delay(streamReadPeriod)
                    continue
                }

                var processingRecordTimestamp: Long

                eventsBatch.forEach { eventRecord ->
                    processingRecordTimestamp = eventRecord.createdAt

                    feedToHandling(eventStoreReader.getReadIndex(), eventRecord) {
                        eventStreamNotifier.onRecordHandledSuccessfully(streamName, eventRecord.eventTitle)

                        if (processedRecords++ % 10 == 0L)
                            eventStoreReader.commitReadIndex(processingRecordTimestamp)
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

    override fun launchEventStream() {
        if (!active.compareAndSet(false, true)) {
            logger.warn("Failed to CAS active state to 'true' for stream $streamName.")
            return
        }

        eventStreamJob = launchJob()
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
        logger.info("Resuming stream $streamName...")
        suspended.set(false)
    }

    override fun resetToReadingIndex(version: Long) {
        if (version < 1) throw IllegalArgumentException("Can't reset to non existing version: $version")
        eventStoreReader.resetReadIndex(ReadIndexResetInfo(version))
    }

    class EventConsumedAck(
        val readIndex: Long,
        val successful: Boolean,
    )

    class EventRecordForHandling(
        val readIndex: Long,
        val record: EventRecord,
    )
}

class ReadIndexResetInfo(
    val resetIndex: Long
)

interface EventStoreReader {
    fun read(): List<EventRecord>
    fun getReadIndex(): Long
    fun commitReadIndex(index: Long)
    fun resetReadIndex(resetInfo: ReadIndexResetInfo)
    fun stop()
}

class SingleEventStoreReader(
    private val eventStore: EventStore,
    private val tableName: String,
    private val streamName: String,
    private val batchSize: Int,
    private val eventStreamNotifier: EventStreamNotifier,
    private val dispatcher: CoroutineDispatcher
) : EventStoreReader {
    companion object {
        private val NO_RESET_REQUIRED = ReadIndexResetInfo(-1)
    }

    private val logger: Logger = LoggerFactory.getLogger(SingleEventStoreReader::class.java)
    private val nextReaderAliveCheck: Duration = 15.seconds

    private val streamManager: EventStreamReaderManager = ActiveEventStreamReaderManager(eventStore)
    private val readIndex: AtomicReference<EventStreamReadIndex> = AtomicReference(null)
    private val isReaderPrimary: AtomicBoolean = AtomicBoolean(false)
    private val isScanActive: Boolean = true

    private val scanJob: Job = launchEventStoreReaderScanJob()

    private var indexResetInfo: ReadIndexResetInfo = NO_RESET_REQUIRED

    override fun read(): List<EventRecord> {
        if (!isReaderPrimary.get())
            return emptyList()

        checkAndResetIndexIfRequired()

        val currentReadIndex = readIndex.get()
        val eventsBatch = eventStore.findBatchOfEventRecordAfter(tableName, currentReadIndex.readIndex, batchSize)

        eventStreamNotifier.onBatchRead(streamName, eventsBatch.size)
        return eventsBatch
    }

    override fun getReadIndex(): Long {
        val currentReadIndex = readIndex.get()
        return currentReadIndex.readIndex
    }

    override fun commitReadIndex(index: Long) {
        val currentReadIndex = readIndex.get()

        EventStreamReadIndex(streamName, index, currentReadIndex.version + 1L).also {
            logger.info("Committing index for $streamName, index: ${index}, current version: ${currentReadIndex.version}")
            eventStore.commitStreamReadIndex(it)
            eventStreamNotifier.onReadIndexCommitted(streamName, it.readIndex)
        }

        syncReaderIndex()
    }

    override fun resetReadIndex(resetInfo: ReadIndexResetInfo) {
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
                } else if (streamManager.tryInterceptReading(streamName)) {
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

    private fun syncReaderIndex() {
        eventStore.findStreamReadIndex(streamName)?.also {
            readIndex.set(it)
            logger.info("Reader index synced for $streamName. Index: ${it.readIndex}, version: ${it.version}")
            eventStreamNotifier.onReadIndexSynced(streamName, it.readIndex)
        }
    }

    private fun checkAndResetIndexIfRequired() {
        if (indexResetInfo != NO_RESET_REQUIRED) {
            val currentReadIndex = readIndex.get()
            val updatedReadIndex = EventStreamReadIndex(streamName, indexResetInfo.resetIndex, currentReadIndex.version)

            readIndex.set(updatedReadIndex)
            logger.warn("Index for stream $streamName forcibly reset to ${indexResetInfo.resetIndex}")

            indexResetInfo = NO_RESET_REQUIRED
            eventStreamNotifier.onStreamReset(streamName, indexResetInfo.resetIndex)
        }
    }
}