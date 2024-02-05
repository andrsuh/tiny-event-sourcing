package ru.quipy.kafka.streams

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.domain.Topic
import ru.quipy.kafka.core.OngoingGroupManager
import ru.quipy.kafka.registry.DomainGroupRegistry
import ru.quipy.mapper.EventMapper
import ru.quipy.streams.AggregateEventStream
import ru.quipy.streams.StoppableAndDestructible
import java.util.concurrent.Executors
import kotlin.reflect.KClass

/**
 * [KafkaProducerSubscriber] is a wrapper around [AggregateEventStream] which allows to subscribe to the stream and handle the events from it.
 *
 * The main purpose of this class is to handle the events from the stream and send them to the Kafka topic.
 */
class KafkaProducerSubscriber<A : Aggregate, T : Topic>(
    private val aggregateEventStream: AggregateEventStream<A>,
    private val kafkaProducer: KafkaEventProducer<T>,
    private val groupRegistry: DomainGroupRegistry,
    private val ongoingGroupManager: OngoingGroupManager,
    private val eventMapper: EventMapper,
    private val nameToEventClassFunc: (String) -> KClass<Event<A>>
) : StoppableAndDestructible {

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(KafkaProducerSubscriber::class.java)
    }

    @Volatile
    private var active = true

    private val subscriptionCoroutine: Job = CoroutineScope(
        CoroutineName("handlingCoroutine") + Executors.newSingleThreadExecutor()
            .asCoroutineDispatcher()
    ).launch {
        while (active) {
            aggregateEventStream.handleNextRecord { eventRecord ->
                val result = runCatching {
                    val event = payloadToEvent(eventRecord.payload, eventRecord.eventTitle)
                    val domainGroup = groupRegistry.getGroupFromDomainEvent(event::class)

                    val aggregationTable = "aggregation-${domainGroup?.simpleName?.lowercase()}-${eventRecord.aggregateId}"
                    val externalEventRecords = if (domainGroup == null) {
                        ongoingGroupManager.convertDomainEvent(event)
                    } else {
                        ongoingGroupManager.convertDomainGroup(event, domainGroup, aggregationTable)
                    }

                    if (externalEventRecords != null) {
                        if (externalEventRecords.isNotEmpty()) {
                            kafkaProducer.sendEvents(eventRecord.aggregateId.toString(), externalEventRecords)
                            logger.info("Sent ${externalEventRecords.size} event(s) to Kafka")
                            if (externalEventRecords.size > 1) {
                                ongoingGroupManager.removeAggregationTable(aggregationTable)
                            }
                        }
                    }

                    true
                }.onFailure { e ->
                    logger.error(
                        "Unexpected exception while handling event in subscriber. Stream: ${aggregateEventStream.streamName}, event record: $eventRecord",
                        e
                    )
                }

                result.getOrDefault(false)
            }
        }

    }

    private fun payloadToEvent(payload: String, eventTitle: String): Event<A> = eventMapper.toEvent(
        payload,
        nameToEventClassFunc(eventTitle)
    )

    override fun stopAndDestroy() {
        active = false
        subscriptionCoroutine.cancel()
        aggregateEventStream.stopAndDestroy()
        kafkaProducer.close()
    }
}