package ru.quipy.kafka.streams

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.Topic
import ru.quipy.mapper.ExternalEventMapper
import ru.quipy.streams.EventStreamSubscriber
import ru.quipy.streams.StoppableAndDestructible
import java.util.concurrent.Executors
import kotlin.reflect.KClass

class KafkaConsumerSubscriber<T : Topic>(
    private val externalEventStream: KafkaConsumerEventStream<T>,
    private val eventMapper: ExternalEventMapper,
    private val nameToEventClassFunc: (String) -> KClass<ExternalEvent<T>>,
    private val handlers: Map<KClass<out ExternalEvent<T>>, suspend (ExternalEvent<T>) -> Unit>,
) : StoppableAndDestructible {

    @Volatile
    private var active = true

    private val logger: Logger = LoggerFactory.getLogger(EventStreamSubscriber::class.java)

    private val subscriptionCoroutine: Job = CoroutineScope(
        CoroutineName("handlingCoroutine") + Executors.newSingleThreadExecutor()
            .asCoroutineDispatcher()
    ).launch {
        while (active) {
            externalEventStream.handleNextRecord { externalEventRecord ->
                try {
                    val externalEvent = payloadToEvent(externalEventRecord.payload, externalEventRecord.eventTitle)
                    handlers[externalEvent::class]?.invoke(externalEvent)
                    true
                } catch (e: Exception) {
                    logger.error(
                        "Unexpected exception while handling event in subscriber. Stream: ${externalEventStream.streamName}, event record: $externalEventRecord",
                        e
                    )
                    false
                }
            }
        }
    }

    private fun payloadToEvent(payload: String, eventTitle: String): ExternalEvent<T> = eventMapper.toEvent(
        payload,
        nameToEventClassFunc(eventTitle)
    )

    override fun stopAndDestroy() {
        active = false
        subscriptionCoroutine.cancel()
        externalEventStream.stopAndDestroy()
    }

    class EventStreamSubscriptionBuilder<T : Topic>(
        private val wrapped: KafkaConsumerEventStream<T>,
        private val externalEventMapper: ExternalEventMapper,
        private val nameToEventClassFunc: (String) -> KClass<ExternalEvent<T>>,
    ) {
        private val handlers = mutableMapOf<KClass<out ExternalEvent<T>>, suspend (ExternalEvent<T>) -> Unit>()

        fun <E : ExternalEvent<T>> `when`(
            eventType: KClass<E>,
            eventHandler: suspend (E) -> Unit
        ): EventStreamSubscriptionBuilder<T> {
            handlers[eventType] = eventHandler as suspend (ExternalEvent<T>) -> Unit
            return this
        }

        fun subscribe() = KafkaConsumerSubscriber(wrapped, externalEventMapper, nameToEventClassFunc, handlers)
    }
}


fun <T : Topic> KafkaConsumerEventStream<T>.toSubscriptionBuilder(
    externalEventMapper: ExternalEventMapper,
    nameToEventClassFunc: (String) -> KClass<ExternalEvent<T>>
) = KafkaConsumerSubscriber.EventStreamSubscriptionBuilder(this, externalEventMapper, nameToEventClassFunc)