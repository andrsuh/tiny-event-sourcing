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
import kotlin.reflect.KClass


// todo sukhoa if we want to have more than one instance we should maintain "active consumers" table
@Deprecated("It's too slow")
class OneByOneReadAggregateEventsStream<A : Aggregate>(
    override val streamName: String,
    private val streamReadPeriod: Long,
    private val tableName: String,
    private val eventMapper: EventMapper,
    private val nameToEventClassFunc: (String) -> KClass<Event<A>>,
    private val eventStoreDbOperations: EventStoreDbOperations,
    private val dispatcher: CoroutineDispatcher
) : AggregateEventsStream<A> {
    companion object {
        private val logger = LoggerFactory.getLogger(OneByOneReadAggregateEventsStream::class.java)
        private val NO_RESET_REQUIRED = ResetInfo(-1)
    }

    private val eventsReadyForProcessingChannel: Channel<EventForProcessing<A>> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    private val acknowledgesChannel: Channel<EventConsumedAck> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    @Volatile
    private var resetInfo = NO_RESET_REQUIRED

    @Volatile
    private var active = true

    @Volatile
    private var readingIndex = 0L

    @Volatile
    private var readerIndexCommittedVersion = 0L

    @Volatile
    private var processedRecords = 0L

    @Suppress("unused")
    private val readingCoroutine =
        CoroutineScope(CoroutineName("reading-$streamName-coroutine") + dispatcher).launch {
            suspendTillTableExists()
            syncReaderIndex()
            logger.info("Resuming stream $streamName with reading index $readingIndex")

            while (active) {
                try {
                    checkAndResetIfRequired()

                    var processingRecordTimestamp = readingIndex
                    eventStoreDbOperations.findOneEventRecordAfter(tableName, readingIndex)?.let {
                        processingRecordTimestamp = it.createdAt
                        eventMapper.toEvent<A>(
                            it.payload,
                            nameToEventClassFunc(it.eventTitle)
                        )
                    }?.also {
                        eventsReadyForProcessingChannel.send(EventForProcessing(readingIndex, it))
                        if (acknowledgesChannel.receive().processed) {
                            readingIndex = processingRecordTimestamp

                            if (processedRecords++ % 10 == 0L) {
                                commitReaderIndexAndSync()
                            }

                            delay(streamReadPeriod)
                        }
                    }
                } catch (ignored: CancellationException) {
                } catch (th: Throwable) {
                    logger.error("Unexpected error in aggregate event stream ${streamName}. Index $readingIndex", th)
                }
            }
            logger.warn("Stopped event stream coroutine. Stream: $streamName, index $readingIndex")
        } // todo sukhoa on completion handler

    private suspend fun suspendTillTableExists() {
        while (!eventStoreDbOperations.tableExists(tableName)) {
            delay(10_000)
            logger.trace("Event stream $streamName is waiting for $tableName to be created")
        }
    }

    private fun syncReaderIndex() {
        eventStoreDbOperations.findStreamReadIndex(streamName)?.also {
            readingIndex = it.readIndex
            readerIndexCommittedVersion = it.version
            logger.info("Reader index synced for $streamName. Index: $readingIndex, version: $readerIndexCommittedVersion")
        }
    }

    private fun commitReaderIndexAndSync() {
        EventStreamReadIndex(streamName, readingIndex, readerIndexCommittedVersion + 1L).also {
            logger.info("Committing index $readingIndex, current version: $readerIndexCommittedVersion")
            eventStoreDbOperations.commitStreamReadIndex(it)
        }
        syncReaderIndex()
    }

    override suspend fun handleEvent(eventProcessingFunction: suspend (Event<A>) -> Boolean) {
        val receive = eventsReadyForProcessingChannel.receive()
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

    private fun checkAndResetIfRequired() { // todo sukhoa duplicated
        if (resetInfo != NO_RESET_REQUIRED) {
            readingIndex = resetInfo.resetIndex
            logger.warn("Index for stream $streamName forcibly reset to $readingIndex")
            resetInfo = NO_RESET_REQUIRED
        }
    }

    override fun stopAndDestroy() {
        active = false
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