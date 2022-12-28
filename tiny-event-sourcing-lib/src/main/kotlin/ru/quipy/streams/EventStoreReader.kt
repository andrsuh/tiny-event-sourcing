package ru.quipy.streams

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.database.EventStore
import ru.quipy.domain.EventRecord
import ru.quipy.domain.EventStreamReadIndex
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

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