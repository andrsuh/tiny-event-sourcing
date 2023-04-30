package ru.quipy.streams

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingProperties
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

interface EventReader {
    suspend fun read(batchSize: Int): List<EventRecord>
    fun postProcessRecord(eventRecord: EventRecord)

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
    private val tableName: String,
    private val streamManager: EventStreamReaderManager,
    private val config: EventSourcingProperties,
    private val eventStreamNotifier: EventStreamNotifier,
    private val dispatcher: CoroutineDispatcher
) : EventReader {
    companion object {
        private val NO_RESET_REQUIRED = ReadIndexResetInfo(-1)
    }

    private val logger: Logger = LoggerFactory.getLogger(EventStoreReader::class.java)

    private val readerId = UUID.randomUUID().toString()
    private val version: AtomicLong = AtomicLong(1L)

    private val isActiveReader: AtomicBoolean = AtomicBoolean(false)
    private val isHealthcheckActive: AtomicBoolean = AtomicBoolean(true)

    private var healthCheckJob: Job = launchEventStoreReaderHealthCheckJob()

    private var eventStoreReadIndex: EventStreamReadIndex = EventStreamReadIndex(streamName, readIndex = 0L, version = 0L)
    private var indexResetInfo: ReadIndexResetInfo = NO_RESET_REQUIRED
    private var processedRecords = 0L

    override suspend fun read(batchSize: Int): List<EventRecord> {
        if (!isActiveReader.get())
            return emptyList()

        checkAndResetIndexIfRequired()

        val eventRecords = eventStore.findBatchOfEventRecordAfter(tableName, eventStoreReadIndex.readIndex, batchSize)
        eventStreamNotifier.onBatchRead(streamName, eventRecords.size)

        return eventRecords
    }

    override fun postProcessRecord(eventRecord: EventRecord) {
        val processingRecordTimestamp = eventRecord.createdAt

        eventStoreReadIndex = EventStreamReadIndex(streamName, processingRecordTimestamp, eventStoreReadIndex.version)

        if (processedRecords++ % config.recordReadIndexCommitPeriod == 0L)
            commitReadIndex(processingRecordTimestamp)
    }

    override fun resetReadIndex(resetInfo: ReadIndexResetInfo) {
        if (eventStoreReadIndex.version < 1)
            throw IllegalArgumentException("Can't reset to non existing version: ${eventStoreReadIndex.version}")

        indexResetInfo = resetInfo
    }

    override fun stop() {
        if (isActiveReader.get())
            isActiveReader.set(false)

        if (isHealthcheckActive.get()) {
            isHealthcheckActive.set(false)
            healthCheckJob.cancel()
        }
    }

    override fun resume() {
        healthCheckJob = launchEventStoreReaderHealthCheckJob()
    }

    private fun commitReadIndex(index: Long) {
        EventStreamReadIndex(streamName, index, eventStoreReadIndex.version + 1L).also {
            logger.info("Committing index for $streamName, index: ${index}, current version: ${eventStoreReadIndex.version}")
            eventStore.commitStreamReadIndex(it)
            eventStreamNotifier.onReadIndexCommitted(streamName, it.readIndex)
        }

        syncReaderIndex()
    }

    private fun launchEventStoreReaderHealthCheckJob(): Job {
        return CoroutineScope(CoroutineName("reading-$streamName-event-store-coroutine") + dispatcher).launch {
            while (isHealthcheckActive.get()) {
                val activeReader: ActiveEventStreamReader? = streamManager.findActiveReader(streamName)

                if (activeReader?.readerId == readerId) {
                    logger.debug("Current reader is active reader of stream $streamName and is alive. Updating its state...")
                    if (streamManager.tryUpdateReaderState(streamName, readerId, eventStoreReadIndex.readIndex)) {
                        delay(config.eventReaderHealthCheckPeriod.inWholeMilliseconds)
                        continue
                    }
                }

                if (streamManager.hasActiveReader(streamName)) {
                    logger.debug("Reader of stream $streamName is alive. Waiting ${config.eventReaderHealthCheckPeriod.inWholeMilliseconds} before continuing...")
                    delay(config.eventReaderHealthCheckPeriod.inWholeMilliseconds)
                    continue
                }

                if (streamManager.tryInterceptReading(streamName, readerId)) {
                    ensureTableExists()
                    syncReaderIndex()
                    isActiveReader.set(true)
                } else {
                    logger.debug("Failed to intercept reading of stream $streamName, because someone else succeeded first.")
                }
            }
        }.also {
            it.invokeOnCompletion { th: Throwable? ->
                if (isHealthcheckActive.get()) {
                    logger.error("Unexpected error in event store reader ${streamName}. Relaunching...", th)
                    healthCheckJob = launchEventStoreReaderHealthCheckJob()
                } else {
                    logger.warn("Stopped event store reader coroutine of stream $streamName")
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
            eventStoreReadIndex = it
            logger.info("Reader index synced for $streamName. Index: ${it.readIndex}, version: ${it.version}")
            eventStreamNotifier.onReadIndexSynced(streamName, it.readIndex)
        }
    }

    private fun checkAndResetIndexIfRequired() {
        if (indexResetInfo != NO_RESET_REQUIRED) {
            val updatedReadIndex = EventStreamReadIndex(streamName, indexResetInfo.resetIndex, eventStoreReadIndex.version)

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