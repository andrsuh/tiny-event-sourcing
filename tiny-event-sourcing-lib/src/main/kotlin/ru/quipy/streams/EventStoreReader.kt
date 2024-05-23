package ru.quipy.streams

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.Aggregate
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.utils.NamedThreadFactory
import java.util.*
import java.util.concurrent.Executors

/**
 * Allows to read batches of event-records from some underlying storage (event bus, event store).
 * It shouldn't be shared among several threads as this will break the order of the stream events.
 * And the class is not basically thread-safe since it's not needed to be such by his nature.
 */
interface EventReader {
    suspend fun read(batchSize: Int): List<EventRecord>

    /**
     * Used to inform that the given record is successfully consumed.
     * NOTE!!! The records should be acked in the same order they were read from the stream
     */
    suspend fun acknowledgeRecord(eventRecord: EventRecord)

    /**
     * We can "replay" events in the stream by resetting it to desired reading index
     */
    fun resetReadIndex(resetInfo: ReadIndexResetInfo)
    fun stop()
    fun resume()
}

class EventStoreReader(
    private val eventStore: EventStore,
    private val streamName: String,
    aggregateInfo: AggregateRegistry.BasicAggregateInfo<Aggregate>,
    private val streamManager: EventStreamReaderManager,
    config: EventSourcingProperties,
    private val eventStreamNotifier: EventStreamNotifier,
    private val dispatcher: CoroutineDispatcher
) : EventReader {

    private val eventStoreTableName = aggregateInfo.aggregateEventsTableName

    private val readerCommitPeriodMessages = config.readerCommitPeriodMessages
    private val readerCommitPeriodMillis = config.readerCommitPeriodMillis

    private val healthcheckPeriodInMillis =
        config.eventReaderHealthCheckPeriod.inWholeMilliseconds + kotlin.random.Random.nextLong(config.eventReaderHealthCheckPeriod.inWholeMilliseconds / 5)

    companion object {
        private val NO_RESET_REQUIRED = ReadIndexResetInfo(-1)
    }

    private val logger: Logger = LoggerFactory.getLogger(EventStoreReader::class.java)

    private val readerId = UUID.randomUUID().toString()

    @Volatile
    private var lastCommittedTs: Long = 0L
    @Volatile
    private var lastCommittedIndex: Long = 0L

    // Relevant in cluster mode when there are multiple instances of the app and respectively the multiple instances of the
    // readers at the same time. Only one of them can be "active" at any time
    @Volatile
    private var meIsActiveReader: Boolean = false

    @Volatile
    private var isHealthcheckActive: Boolean = true

    private var healthCheckJob: Job = launchEventStoreReaderHealthCheckJob()

    @Volatile
    private var eventStoreReadIndex: EventStreamReadIndex =
        EventStreamReadIndex(streamName, readIndex = 0L, version = 0L)

    // This variable can signal that someone requested the "reset" of the stream.
    // Reset enable the stream to "replay" events that have already been processed
    private var indexResetInfo: ReadIndexResetInfo = NO_RESET_REQUIRED
    @Volatile
    private var processedRecords = 0L

    private val readIndexMutex = Mutex()

    private val eventStreamCompletionHandler: CompletionHandler = { th: Throwable? ->
        if (th != null) {
            logger.error(
                "Unexpected error in reader commit coroutine ${streamName}. Relaunching...",
                th
            )

            readerCommitJob = launchJob()
        } else {
            logger.warn("Stopped reader commit coroutine $streamName")
        }
    }

    @Volatile
    private var readerCommitJob: Job = launchJob()
    private fun launchJob() =
        CoroutineScope(CoroutineName("reader-commit-$streamName-coroutine") + Executors.newSingleThreadExecutor(
            NamedThreadFactory("$streamName-reader-committer")
        ).asCoroutineDispatcher()).launch {
            while (true) {
                if (!meIsActiveReader || lastCommittedIndex == eventStoreReadIndex.readIndex) {
                    delay(readerCommitPeriodMillis)
                    continue
                }

                if (processedRecords % readerCommitPeriodMessages == 0L
                    || System.currentTimeMillis() - lastCommittedTs > readerCommitPeriodMillis
                ) {
                    commitReadIndex()
                } else {
                    delay(readerCommitPeriodMillis)
                    continue
                }
            }
        }.also {
            it.invokeOnCompletion(eventStreamCompletionHandler)
        }

    override suspend fun read(batchSize: Int): List<EventRecord> {
        if (!meIsActiveReader) {
            logger.debug("Skip reading by reader id $readerId, stream $streamName has another active reader")
            return emptyList()
        }

        checkAndResetIndexIfRequired()

        val eventRecords =
            eventStore.findBatchOfEventRecordAfter(eventStoreTableName, eventStoreReadIndex.readIndex, batchSize)
        eventStreamNotifier.onBatchRead(streamName, eventRecords.size)

        return eventRecords
    }

    override suspend fun acknowledgeRecord(eventRecord: EventRecord) {
        if (!meIsActiveReader || eventRecord.createdAt < eventStoreReadIndex.readIndex) return

        readIndexMutex.withLock {
            eventStoreReadIndex = eventStoreReadIndex.copy(readIndex = eventRecord.createdAt)
        }

        processedRecords++
    }

    override fun resetReadIndex(resetInfo: ReadIndexResetInfo) {
        if (eventStoreReadIndex.version < 1)
            throw IllegalArgumentException("Can't reset to non existing version: ${eventStoreReadIndex.version}")

        indexResetInfo = resetInfo
    }

    override fun stop() {
        if (meIsActiveReader) meIsActiveReader = false

        if (isHealthcheckActive) {
            isHealthcheckActive = false
            healthCheckJob.cancel()
        }
    }

    override fun resume() {
        isHealthcheckActive = true
        healthCheckJob = launchEventStoreReaderHealthCheckJob()
    }

    private suspend fun commitReadIndex() {
        val cachedIndex = eventStoreReadIndex

        EventStreamReadIndex(streamName, cachedIndex.readIndex, cachedIndex.version + 1L).let {
            logger.warn("Committing index for $streamName-$readerId, index: ${it.readIndex}, updated version: ${it.version}")
            eventStore.commitStreamReadIndex(it)
            readIndexMutex.withLock {
                // update only the version as the index could have been increased by reading process
                eventStoreReadIndex = eventStoreReadIndex.copy(version = it.version)
            }
            lastCommittedTs = System.currentTimeMillis()
            lastCommittedIndex = it.readIndex

            eventStreamNotifier.onReadIndexCommitted(streamName, it.readIndex)
        }

//        syncReaderIndex()
    }

    private fun launchEventStoreReaderHealthCheckJob(): Job {
        return CoroutineScope(CoroutineName("reading-$streamName-event-store-coroutine") + dispatcher).launch {
            delay(healthcheckPeriodInMillis) // initial delay to handle the simultaneous stream reader starts
            while (isHealthcheckActive) {
                val activeReader: ActiveEventStreamReader? = streamManager.findActiveReader(streamName)

                if (activeReader.isMe()) {
                    logger.debug("Current reader $readerId is active reader of stream $streamName and is alive. Updating its state...")
                    if (performHealthCheck()) {
                        delay(healthcheckPeriodInMillis)
                    }
                } else if (streamManager.hasActiveReader(streamName)) {
                    logger.debug("Reader of stream $streamName is alive. Waiting $healthcheckPeriodInMillis ms before continuing...")
                    delay(healthcheckPeriodInMillis)
                } else if (streamManager.tryInterceptReading(streamName, readerId)) {
                    ensureTableExists()
                    syncReaderIndex()
                    meIsActiveReader = true
                    logger.info("Current reader is promoted to active stream $streamName reader $readerId")
                } else {
                    logger.debug("Failed to intercept reading of stream $streamName id=$readerId, because someone else succeeded first.")
                }
            }
        }.also {
            it.invokeOnCompletion { th: Throwable? ->
                if (isHealthcheckActive) {
                    logger.error("Unexpected error in event store reader ${streamName}. Relaunching...", th)
                    healthCheckJob = launchEventStoreReaderHealthCheckJob()
                } else {
                    logger.warn("Stopped event store reader coroutine of stream $streamName")
                }
            }
        }
    }

    private fun performHealthCheck(): Boolean {
        return streamManager.tryUpdateReaderState(streamName, readerId, eventStoreReadIndex.readIndex)
    }

    private fun ActiveEventStreamReader?.isMe() = this != null && readerId == this@EventStoreReader.readerId

    private suspend fun ensureTableExists() {
        while (!eventStore.tableExists(eventStoreTableName)) {
            delay(2_000)
            logger.trace("Event stream $streamName is waiting for $eventStoreTableName to be created")
        }
    }

    private fun syncReaderIndex() {
        eventStore.findStreamReadIndex(streamName)?.also {
            eventStoreReadIndex = it
            logger.debug("Reader index synced for $streamName. Index: ${it.readIndex}, version: ${it.version}")
            eventStreamNotifier.onReadIndexSynced(streamName, it.readIndex)
        }
    }

    // todo sukhoa looks like not used Supposed to be used when we need to "replay" things
    private fun checkAndResetIndexIfRequired() {
        if (indexResetInfo != NO_RESET_REQUIRED) {
            val updatedReadIndex =
                EventStreamReadIndex(streamName, indexResetInfo.resetIndex, eventStoreReadIndex.version)

            eventStoreReadIndex = updatedReadIndex
            logger.warn("Index for stream $streamName forcibly reset to ${indexResetInfo.resetIndex}")

            indexResetInfo = NO_RESET_REQUIRED
            eventStreamNotifier.onStreamReset(streamName, indexResetInfo.resetIndex)
        }
    }
}

class ReadIndexResetInfo(
    val resetIndex: Long
)