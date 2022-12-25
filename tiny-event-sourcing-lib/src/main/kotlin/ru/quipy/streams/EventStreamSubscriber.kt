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
import kotlin.reflect.KFunction1

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
    private val readerStrategy: EventStreamReadingStrategy<A>
) {
    @Volatile
    private var active = true

    private val logger: Logger = LoggerFactory.getLogger(EventStreamSubscriber::class.java)

    private val subscriptionCoroutine: Job = CoroutineScope(
        CoroutineName("handlingCoroutine") + Executors.newSingleThreadExecutor()
            .asCoroutineDispatcher() // todo sukhoa customize
    ).launch {
        readerStrategy.read(aggregateEventStream)
    }

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
        private val nameToEventClassFunc: KFunction1<String, KClass<Event<A>>>,
        private val activeReaderManager: EventStreamReaderManager,
    ) {
        private val handlers = mutableMapOf<KClass<out Event<A>>, suspend (Event<A>) -> Unit>()

        fun <E : Event<A>> `when`(
            eventType: KClass<E>,
            eventHandler: suspend (E) -> Unit
        ): EventStreamSubscriptionBuilder<A> {
            handlers[eventType] = eventHandler as suspend (Event<A>) -> Unit
            return this
        }

        fun subscribe() = EventStreamSubscriber(wrapped, SingleEventStreamReadingStrategy(activeReaderManager, eventMapper, nameToEventClassFunc, handlers))
    }
}

/**
 * Creates new [EventStreamSubscriptionBuilder] which can be used then to initialize and start [EventStreamSubscriber]
 */
fun <A : Aggregate> AggregateEventStream<A>.toSubscriptionBuilder(
    eventMapper: EventMapper,
    nameToEventClassFunc: KFunction1<String, KClass<Event<A>>>,
    activeReaderManager: EventStreamReaderManager
) = EventStreamSubscriptionBuilder(this, eventMapper, nameToEventClassFunc, activeReaderManager)