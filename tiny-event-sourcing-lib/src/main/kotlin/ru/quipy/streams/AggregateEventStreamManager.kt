package ru.quipy.streams

import kotlinx.coroutines.asCoroutineDispatcher
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.Aggregate
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.reflect.KClass


class AggregateEventStreamManager(
    private val aggregateRegistry: AggregateRegistry,
    private val eventStoreDbOperations: EventStoreDbOperations,
    private val eventSourcingProperties: EventSourcingProperties,
) {
    private val eventStreamListener: EventStreamListenerImpl = EventStreamListenerImpl()// todo sukhoa make injectable

    private val eventStreamsDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher() // todo sukhoa fix

    private val eventStreams = ConcurrentHashMap<String, AggregateEventStream<*>>()

    fun <A : Aggregate> createEventStream(
        streamName: String,
        aggregateClass: KClass<A>,
        retryConfig: RetryConf = RetryConf(3, RetryFailedStrategy.SKIP_EVENT)
    ): AggregateEventStream<A> {
        val eventInfo = (aggregateRegistry.getEventInfo(aggregateClass)
            ?: throw IllegalArgumentException("Aggregate $aggregateClass is not registered"))

        val existing = eventStreams.putIfAbsent(
            streamName, BufferedAggregateEventStream<A>(
                streamName,
                eventSourcingProperties.streamReadPeriod,
                eventSourcingProperties.streamBatchSize,
                eventInfo.aggregateEventsTableName,
                retryConfig,
                eventStoreDbOperations,
                eventStreamListener,
                eventStreamsDispatcher
            )
        )

        if (existing != null) throw IllegalStateException("There is already stream $streamName for aggregate ${aggregateClass.simpleName}")

        return eventStreams[streamName] as AggregateEventStream<A>
    }

    fun destroy() {
        eventStreams.values.forEach {
            it.stopAndDestroy()
        }
    }

    fun maintenance(block: EventStreamListener.() -> Unit) { // todo sukhoa naming
        block(eventStreamListener)
    }

    fun streamsInfo() = eventStreams.map { (_, stream) ->
        StreamInfo(
            stream.streamName,
            stream.readingIndex
        )
    }.toList()

    fun getStreamByName(name: String) = eventStreams[name]

    data class StreamInfo(
        val streamName: String,
        val readingIndex: Long
    )
}