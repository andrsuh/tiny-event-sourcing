package ru.quipy.kafka.streams

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.domain.Topic
import ru.quipy.kafka.core.OngoingGroupManager
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
    private val ongoingGroupManager: OngoingGroupManager,
    private val eventMapper: EventMapper,
    private val nameToEventClassFunc: (String) -> KClass<Event<A>>
) : StoppableAndDestructible {

    @Volatile
    private var active = true

    private val logger: Logger = LoggerFactory.getLogger(KafkaProducerSubscriber::class.java)

    private val subscriptionCoroutine: Job = CoroutineScope(
        CoroutineName("handlingCoroutine") + Executors.newSingleThreadExecutor()
            .asCoroutineDispatcher() // todo sukhoa customize
    ).launch {
        while (active) {
            aggregateEventStream.handleNextRecord { eventRecord ->
                try {
                    val event = payloadToEvent(eventRecord.payload, eventRecord.eventTitle)

                    val externalEventRecords = ongoingGroupManager.convertToExternalEventRecords(event, eventRecord.aggregateId.toString())

                    kafkaProducer.sendEvents(eventRecord.aggregateId.toString(), externalEventRecords)

                    true
                } catch (e: Exception) {
                    logger.error(
                        "Unexpected exception while handling event in subscriber. Stream: ${aggregateEventStream.streamName}, event record: $eventRecord",
                        e
                    )
                    false
                }
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