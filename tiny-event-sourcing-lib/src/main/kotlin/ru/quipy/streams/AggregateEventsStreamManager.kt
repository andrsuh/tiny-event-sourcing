package ru.quipy.streams

import kotlinx.coroutines.asCoroutineDispatcher
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.Aggregate
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.reflect.KClass


class AggregateEventsStreamManager(
    private val aggregateRegistry: AggregateRegistry,
    private val eventStoreDbOperations: EventStoreDbOperations,
    private val eventSourcingProperties: EventSourcingProperties
) {
    private val eventStreamsDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher() // todo sukhoa fix

    private val eventStreams = ConcurrentHashMap<StreamId, AggregateEventsStream<*>>()

    fun <A : Aggregate> createEventStream(
        streamName: String,
        aggregateClass: KClass<A>,
        retryConfig: RetryConf = RetryConf(3, RetryFailedStrategy.SKIP_EVENT)
    ): AggregateEventsStream<A> {
        val aggregateInfo = (aggregateRegistry.getAggregateInfo(aggregateClass)
            ?: throw IllegalArgumentException("Aggregate $aggregateClass is not registered"))

        val streamId = StreamId(streamName, aggregateClass)
        val existing = eventStreams.putIfAbsent(
            streamId, BufferedAggregateEventsStream<A>(
                streamName,
                eventSourcingProperties.streamReadPeriod,
                eventSourcingProperties.streamBatchSize,
                aggregateInfo.aggregateEventsTableName,
                retryConfig,
                eventStoreDbOperations,
                eventStreamsDispatcher
            )
        )

        if (existing != null) throw IllegalStateException("There is already stream $streamName for aggregate ${aggregateClass.simpleName}")

        return eventStreams[streamId] as AggregateEventsStream<A>
    }

    fun destroy() {
        eventStreams.values.forEach {
            it.stopAndDestroy()
        }
    }

    private data class StreamId(
        val streamName: String, val aggregateClass: KClass<*>
    )
}