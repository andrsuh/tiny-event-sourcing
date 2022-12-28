package ru.quipy.streams

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.mapper.EventMapper
import ru.quipy.streams.EventStreamSubscriber.EventStreamSubscriptionBuilder
import java.util.concurrent.Executors
import kotlin.reflect.KClass

/**
 * Wraps the instance of [AggregateEventStream] and:
 *  - Handles event records from underlying stream, turn them into [Event] instances
 *  - Holds the handlers map which maps some certain type of [Event] to the logic that should be performed to handle it.
 *
 * We do not recommend to create instances of EventStreamSubscriber directly through constructor. Preferred way is to
 * use [AggregateSubscriptionsManager] methods.
 */
class EventStreamSubscriber<A : Aggregate>(
    /**
     * Wrapped [AggregateEventStream]
     */
    private val aggregateEventStream: AggregateEventStream<A>,
    /**
     * Allows to map some row representation of event to instance of [Event] class
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
    private val handlers: Map<KClass<out Event<A>>, suspend (Event<A>) -> Unit>
) {
    @Volatile
    private var active = true

    private val logger: Logger = LoggerFactory.getLogger(EventStreamSubscriber::class.java)

    private val subscriptionCoroutine: Job = CoroutineScope(
        CoroutineName("handlingCoroutine") + Executors.newSingleThreadExecutor()
            .asCoroutineDispatcher() // todo sukhoa customize
    ).launch {
        while (active) {
            aggregateEventStream.handleNextRecord { eventRecord ->
                try {
                    val event = payloadToEvent(eventRecord.payload, eventRecord.eventTitle)
                    handlers[event::class]?.invoke(event)
                    true
                } catch (e: Exception) {
                    logger.error("Unexpected exception while handling event in subscriber. Stream: ${aggregateEventStream.streamName}, event record: $eventRecord", e)
                    false
                }
            }
        }
    }

    private fun payloadToEvent(payload: String, eventTitle: String): Event<A> = eventMapper.toEvent(
        payload,
        nameToEventClassFunc(eventTitle)
    )

    /**
     * Stops both the consuming process and the underlying event stream process
     */
    fun stopAndDestroy() {
        active = false
        subscriptionCoroutine.cancel()
        aggregateEventStream.stopAndDestroy()
    }

    /**
     * Allows to build new instance of [EventStreamSubscriber].
     */
    class EventStreamSubscriptionBuilder<A : Aggregate>(
        private val wrapped: AggregateEventStream<A>,
        private val eventMapper: EventMapper,
        private val nameToEventClassFunc: (String) -> KClass<Event<A>>,
    ) {
        private val handlers = mutableMapOf<KClass<out Event<A>>, suspend (Event<A>) -> Unit>()

        fun <E : Event<A>> `when`(
            eventType: KClass<E>,
            eventHandler: suspend (E) -> Unit
        ): EventStreamSubscriptionBuilder<A> {
            handlers[eventType] = eventHandler as suspend (Event<A>) -> Unit
            return this
        }

        fun subscribe() = EventStreamSubscriber(wrapped, eventMapper, nameToEventClassFunc, handlers)
    }
}

/**
 * Creates new [EventStreamSubscriptionBuilder] which can be used then to initialize and start [EventStreamSubscriber]
 */
fun <A : Aggregate> AggregateEventStream<A>.toSubscriptionBuilder(
    eventMapper: EventMapper,
    nameToEventClassFunc: (String) -> KClass<Event<A>>
) = EventStreamSubscriptionBuilder(this, eventMapper, nameToEventClassFunc)