package ru.quipy.streams

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.mapper.EventMapper
import java.util.*
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

interface EventStreamReadingStrategy<A : Aggregate> {
    suspend fun read(stream: AggregateEventStream<A>)
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
        // TODO: stop mechanism
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
}

class SingleEventStreamReadingStrategy<A : Aggregate>(
    private val streamManager: EventStreamReaderManager,
    private val eventMapper: EventMapper,
    private val nameToEventClassFunc: (String) -> KClass<Event<A>>,
    private val handlers: Map<KClass<out Event<A>>, suspend (Event<A>) -> Unit>,
) : EventStreamReadingStrategy<A> {
    private val logger: Logger = LoggerFactory.getLogger(SingleEventStreamReadingStrategy::class.java)
    private val nextReaderAliveCheck: Duration = 1.seconds

    override suspend fun read(stream: AggregateEventStream<A>) {
        while (true) {
            if (streamManager.isReaderAlive(stream.streamName)) {
                Thread.sleep(nextReaderAliveCheck.inWholeMilliseconds)
            } else if (streamManager.tryInterceptReading(stream.streamName)) {
                val commonReader = CommonEventStreamReadingStrategy(streamManager, eventMapper, nameToEventClassFunc, handlers)
                commonReader.read(stream)
            } else continue
        }
    }
}

interface EventStreamReaderManager {
    fun isReaderAlive(streamName: String): Boolean
    fun tryInterceptReading(streamName: String): Boolean
    fun updateReaderState(streamName: String, readingIndex: Long)
}

class ActiveEventStreamReaderManager(
    private val eventStore: EventStore,
) : EventStreamReaderManager {
    private val logger: Logger = LoggerFactory.getLogger(ActiveEventStreamReaderManager::class.java)
    private val maxActiveReaderInactivityPeriod: Duration = 5.minutes

    override fun isReaderAlive(streamName: String): Boolean {
        val activeStreamReader: ActiveEventStreamReader = eventStore.getActiveStreamReader(streamName) ?: return false
        val lastInteraction = activeStreamReader.lastInteraction
        val currentTime = System.currentTimeMillis()

        if ((currentTime - lastInteraction).minutes > maxActiveReaderInactivityPeriod)
            return false

        return true
    }

    override fun tryInterceptReading(streamName: String): Boolean {
        val currentActiveReader: ActiveEventStreamReader? = eventStore.getActiveStreamReader(streamName)
        val newActiveReader = createNewActiveReader(streamName, currentActiveReader)

        val expectedVersion = currentActiveReader?.version ?: 0

        if (eventStore.tryReplaceActiveStreamReader(expectedVersion, newActiveReader)) {
            logger.info("Event stream reader of stream $streamName has been switched from [${currentActiveReader?.id}] to [${newActiveReader.id}]")
            return true
        }

        return false
    }

    override fun updateReaderState(streamName: String, readingIndex: Long) {
        val activeReader: ActiveEventStreamReader? = eventStore.getActiveStreamReader(streamName)

        val updatedActiveReader = ActiveEventStreamReader(
            activeReader?.id ?: streamName,
            activeReader?.version ?: 1,
            readingIndex,
            lastInteraction = System.currentTimeMillis(),
        )

        eventStore.updateActiveStreamReader(updatedActiveReader)
    }

    private fun createNewActiveReader(streamName: String, currentActiveReader: ActiveEventStreamReader?): ActiveEventStreamReader {
        val newVersion: Long = (currentActiveReader?.version ?: 1) + 1
        val readPosition: Long = currentActiveReader?.readPosition ?: 1
        val lastInteraction: Long = System.currentTimeMillis()

        return ActiveEventStreamReader(streamName, newVersion, readPosition, lastInteraction)
    }
}