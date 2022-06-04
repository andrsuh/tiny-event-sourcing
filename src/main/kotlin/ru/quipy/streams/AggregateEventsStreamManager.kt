package ru.quipy.streams

import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.domain.Aggregate
import ru.quipy.mapper.EventMapper
import kotlinx.coroutines.asCoroutineDispatcher
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import javax.annotation.PreDestroy
import kotlin.reflect.KClass


// strategy - on fail - retry / stop / continue
@Component
class AggregateEventsStreamManager {

    val eventStreamsDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher() // todo sukhoa fix

    @Autowired
    lateinit var aggregateRegistry: AggregateRegistry

    @Autowired
    lateinit var eventStoreDbOperations: EventStoreDbOperations

    @Autowired
    lateinit var eventMapper: EventMapper

    @Autowired
    lateinit var eventSourcingProperties: EventSourcingProperties

    private val eventStreams = ConcurrentHashMap<StreamId, AggregateEventsStream<*>>()

    fun <A : Aggregate> createEventStream(
        streamName: String,
        aggregateClass: KClass<A>
    ): AggregateEventsStream<A> {
        val aggregateInfo = (aggregateRegistry.getAggregateInfo(aggregateClass)
            ?: throw IllegalArgumentException("Aggregate $aggregateClass is not registered"))

        val streamId = StreamId(streamName, aggregateClass)
        val existing = eventStreams.putIfAbsent(
            streamId, BufferedAggregateEventsStream(
                streamName,
                eventSourcingProperties.streamReadPeriod,
                eventSourcingProperties.streamBatchSize,
                aggregateInfo.aggregateEventsTableName,
                eventMapper,
                aggregateInfo::getEventTypeByName,
                eventStoreDbOperations,
                eventStreamsDispatcher
            )
        )

        if (existing != null)
            throw IllegalStateException("There is already stream $streamName for aggregate ${aggregateClass.simpleName}")

        return eventStreams[streamId] as AggregateEventsStream<A>
    }

    @PreDestroy
    fun destroy() {
        eventStreams.values.forEach {
            it.stopAndDestroy()
        }
    }

    private data class StreamId(
        val streamName:String,
        val aggregateClass: KClass<*>
    )
}