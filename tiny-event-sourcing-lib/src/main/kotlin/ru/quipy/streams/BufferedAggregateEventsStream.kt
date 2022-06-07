package ru.quipy.streams

import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.domain.EventStreamReadIndex
import ru.quipy.mapper.EventMapper
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.reflect.KClass


class BufferedAggregateEventsStream<A : Aggregate>(
    override val streamName: String,
    private val streamReadPeriod: Long, // todo sukhoa wrong naming
    private val streamBatchSize: Int,
    private val tableName: String,
    private val eventMapper: EventMapper,
    private val nameToEventClassFunc: (String) -> KClass<Event<A>>,
    private val eventStoreDbOperations: EventStoreDbOperations,
    private val dispatcher: CoroutineDispatcher
) : AggregateEventsStream<A> {
    companion object {
        private val logger = LoggerFactory.getLogger(BufferedAggregateEventsStream::class.java)
        private val NO_RESET_REQUIRED = ResetInfo(-1)
    }

    private val eventsChannel: Channel<EventForProcessing<A>> = Channel(
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

    @Volatile
    private var readingIndex = 0L

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
            suspendTillTableExists()
            syncReaderIndex() // todo sukhoa we don't use read index after relaunching
            logger.info("Resuming stream $streamName with reading index $readingIndex")

            while (active.get()) {
                checkAndResetIfRequired()

                val eventsBatch =
                    eventStoreDbOperations.findBatchOfEventRecordAfter(tableName, readingIndex, streamBatchSize)

                if (eventsBatch.isEmpty()) {
                    delay(streamReadPeriod)
                }

                var processingRecordTimestamp: Long
                eventsBatch.forEach {
                    processingRecordTimestamp = it.createdAt

                    val event = payloadToEvent(it.payload, it.eventTitle)

                    eventsChannel.send(EventForProcessing(readingIndex, event))
                    if (acknowledgesChannel.receive().processed) {
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

    private suspend fun suspendTillTableExists() {
        while (!eventStoreDbOperations.tableExists(tableName)) {
            delay(2_000)
            logger.trace("Event stream $streamName is waiting for $tableName to be created")
        }
    }

    private fun payloadToEvent(payload: String, eventTitle: String): Event<A> = eventMapper.toEvent(
        payload,
        nameToEventClassFunc(eventTitle)
    )

    private fun syncReaderIndex() {
        eventStoreDbOperations.findStreamReadIndex(streamName)?.also {
            readingIndex = it.readIndex
            readerIndexCommittedVersion = it.version
            logger.info("Reader index synced for $streamName. Index: $readingIndex, version: $readerIndexCommittedVersion")
        }
    }

    private fun commitReaderIndexAndSync() {
        EventStreamReadIndex(streamName, readingIndex, readerIndexCommittedVersion + 1L).also {
            logger.info("Committing index for $streamName, index: $readingIndex, current version: $readerIndexCommittedVersion")
            eventStoreDbOperations.commitStreamReadIndex(it)
        }
        syncReaderIndex()
    }

    private fun checkAndResetIfRequired() {
        if (resetInfo != NO_RESET_REQUIRED) {
            readingIndex = resetInfo.resetIndex
            logger.warn("Index for stream $streamName forcibly reset to $readingIndex")
            resetInfo = NO_RESET_REQUIRED
        }
    }

    override suspend fun handleEvent(eventProcessingFunction: suspend (Event<A>) -> Boolean) {
        val receive = eventsChannel.receive()
        try {
            eventProcessingFunction(receive.event).also {
                if (!it) logger.info("Processing function return false for event: ${receive.event} at index: ${receive.readIndex}`")
                acknowledgesChannel.send(EventConsumedAck(receive.readIndex, it))
            }
        } catch (e: Exception) {
            logger.error(
                "Error while invoking event processing function at index: ${receive.readIndex} event: ${receive.event}",
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

    override fun resetToAggregateVersion(version: Long) {
        if (version < 1) throw IllegalArgumentException("Can't reset to non existing version: $version")
        resetInfo = ResetInfo(version)
    }

    class EventConsumedAck(
        val readIndex: Long,
        val processed: Boolean,
    )

    class EventForProcessing<A : Aggregate>(
        val readIndex: Long,
        val event: Event<A>,
    )

    class ResetInfo(
        val resetIndex: Long
    )
}