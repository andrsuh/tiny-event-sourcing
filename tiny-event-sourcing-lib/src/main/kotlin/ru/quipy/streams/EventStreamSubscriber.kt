package ru.quipy.streams

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.mapper.EventMapper
import java.util.concurrent.Executors
import kotlin.reflect.KClass

class EventStreamSubscriber<A : Aggregate>(
    private val aggregateEventStream: AggregateEventStream<A>,
    private val eventMapper: EventMapper,
    private val nameToEventClassFunc: (String) -> KClass<Event<A>>,
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

fun <A : Aggregate> AggregateEventStream<A>.toSubscriptionBuilder(
    eventMapper: EventMapper,
    nameToEventClassFunc: (String) -> KClass<Event<A>>
) = EventStreamSubscriber.EventStreamSubscriptionBuilder(this, eventMapper, nameToEventClassFunc)