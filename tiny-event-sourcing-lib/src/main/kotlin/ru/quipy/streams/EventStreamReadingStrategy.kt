package ru.quipy.streams

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.mapper.EventMapper
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

interface EventStreamReadingStrategy<A : Aggregate> {
    suspend fun read(stream: AggregateEventStream<A>)
    fun stop()
}

class CommonEventStreamReadingStrategy<A : Aggregate>(
    private val streamManager: EventStreamReaderManager,
    /**
     * Allows to map some row representation of event to instance of [Event] class.
     */
    private val eventMapper: EventMapper,
    /**
     * When we store event to DB we just store the row bytes of the event (most likely json representation).
     * Also, the event meta-information is stored - id, timestamp, aggregateId and the NAME of the event.
     * The NAME is mapped to the class of corresponding event. So we ask this function - which class is mapped to the name?
     * to correctly deserialize the row content of the event.
     */
    private val nameToEventClassFunc: (String) -> KClass<Event<A>>,
    /**
     * Maps the event classes to corresponding business logic that should be performed once the event of the class is fired.
     */
    private val handlers: Map<KClass<out Event<A>>, suspend (Event<A>) -> Unit>,
) : EventStreamReadingStrategy<A> {
    private val logger: Logger = LoggerFactory.getLogger(CommonEventStreamReadingStrategy::class.java)

    @Volatile
    private var isActive = true

    override suspend fun read(stream: AggregateEventStream<A>) {
        logger.info("Starting reading stream ${stream.streamName}...")
        while (isActive) {
            stream.handleNextRecord { eventRecord ->
                try {
                    val event = convertPayloadToEvent(eventRecord.payload, eventRecord.eventTitle)
                    handlers[event::class]?.invoke(event)
                    streamManager.updateReaderState(stream.streamName, stream.readingIndex)
                    true
                } catch (e: Exception) {
                    logger.error("Unexpected exception while handling event in subscriber. Stream: ${stream.streamName}, event record: $eventRecord", e)
                    false
                }
            }
        }
    }

    private fun convertPayloadToEvent(payload: String, eventTitle: String): Event<A> = eventMapper.toEvent(
        payload,
        nameToEventClassFunc(eventTitle)
    )

    override fun stop() {
        isActive = false
    }
}

class SingleEventStreamReadingStrategy<A : Aggregate>(
    private val streamManager: EventStreamReaderManager,
    private val eventMapper: EventMapper,
    private val nameToEventClassFunc: (String) -> KClass<Event<A>>,
    private val handlers: Map<KClass<out Event<A>>, suspend (Event<A>) -> Unit>,
) : EventStreamReadingStrategy<A> {
    private val logger: Logger = LoggerFactory.getLogger(SingleEventStreamReadingStrategy::class.java)
    private val nextReaderAliveCheck: Duration = 10.seconds

    @Volatile
    private var isActive = true

    override suspend fun read(stream: AggregateEventStream<A>) {
        while (isActive) {
            if (streamManager.isReaderAlive(stream.streamName)) {
                logger.debug("Reader of stream ${stream.streamName} is alive. Waiting $nextReaderAliveCheck before continuing...")
                Thread.sleep(nextReaderAliveCheck.inWholeMilliseconds)
            } else if (streamManager.tryInterceptReading(stream.streamName)) {
                val commonReader = CommonEventStreamReadingStrategy(streamManager, eventMapper, nameToEventClassFunc, handlers)
                commonReader.read(stream)
            } else {
                logger.info("Failed to intercept reading of stream ${stream.streamName}, because someone else succeeded first.")
                continue
            }
        }
    }

    override fun stop() {
        isActive = false
    }
}