package ru.quipy.streams

import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import kotlin.reflect.KClass

class EventStreamSubscriber<A : Aggregate>(
    private val aggregateEventsStream: AggregateEventsStream<A>,
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
            aggregateEventsStream.handleEvent { event ->
                try {
                    handlers[event::class]?.invoke(event)
                    true
                } catch(e: Exception) {
                    logger.error("Unexpected exception while handling event in subscriber. Stream: ${aggregateEventsStream.streamName}, event: $event")
                    false
                }
            }
        }
    }

    /**
     * Stops both the consuming process and the underlying event stream process
     */
    fun stopAndDestroy() {
        active = false
        subscriptionCoroutine.cancel()
        aggregateEventsStream.stopAndDestroy()
    }

    class EventStreamSubscriptionBuilder<A : Aggregate>(
        private val wrapped: AggregateEventsStream<A>,
    ) {
        private val handlers = mutableMapOf<KClass<out Event<A>>, suspend (Event<A>) -> Unit>()

        fun <E : Event<A>> `when`(
            eventType: KClass<E>,
            eventHandler: suspend (E) -> Unit
        ): EventStreamSubscriptionBuilder<A> {
            handlers[eventType] = eventHandler as suspend (Event<A>) -> Unit
            return this
        }

        fun subscribe() = EventStreamSubscriber(wrapped, handlers)
    }
}

fun <A : Aggregate, E : Event<A>> AggregateEventsStream<A>.`when`(
    eventType: KClass<E>,
    eventHandler: suspend (E) -> Unit
) = EventStreamSubscriber.EventStreamSubscriptionBuilder(this).`when`(eventType, eventHandler)

fun <A : Aggregate> AggregateEventsStream<A>.toSubscriptionBuilder() =
    EventStreamSubscriber.EventStreamSubscriptionBuilder(this)