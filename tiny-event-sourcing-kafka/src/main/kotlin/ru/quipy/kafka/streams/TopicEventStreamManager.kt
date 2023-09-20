package ru.quipy.kafka.streams

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.asCoroutineDispatcher
import ru.quipy.core.EventSourcingProperties
import ru.quipy.domain.Topic
import ru.quipy.kafka.core.KafkaProperties
import ru.quipy.kafka.registry.TopicRegistry
import ru.quipy.streams.EventStreamListener
import ru.quipy.streams.EventStreamListenerImpl
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.reflect.KClass

/**
 * Manages [KafkaConsumerEventStream]s for different topics and provides methods for their creation and management.
 *
 * [TopicEventStreamManager] is responsible for managing Kafka consumer event streams associated with various topics.
 * It allows you to create and manage these streams, as well as perform maintenance tasks and retrieve information about active streams.
 *
 */
class TopicEventStreamManager(
    private val topicRegistry: TopicRegistry,
    private val eventSourcingProperties: EventSourcingProperties,
    private val kafkaProperties: KafkaProperties
) {
    private val eventStreamListener: EventStreamListenerImpl = EventStreamListenerImpl()

    private val eventStreamsDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher()

    private val eventStreams = ConcurrentHashMap<String, KafkaConsumerEventStream<*>>()

    private val objectMapper = ObjectMapper()

    fun <T : Topic> createKafkaConsumerStream(
        streamName: String,
        topicClass: KClass<T>,
        retryConfig: RetryConf = RetryConf(3, RetryFailedStrategy.SKIP_EVENT)
    ): KafkaConsumerEventStream<T> {
        val internalEventInfo = topicRegistry.getExternalEventInfo(topicClass)
            ?: throw IllegalArgumentException("Couldn't find topic class ${topicClass.simpleName} in registry")

        val topicName = topicRegistry.basicTopicInfo(topicClass)?.topicName.toString()

        val eventsChannel = ExternalEventsChannel()

        val kafkaConsumer = KafkaEventConsumer<T>(topicName, streamName, kafkaProperties, objectMapper)

        val existing = eventStreams.putIfAbsent(
            streamName, KafkaConsumerEventStream(
                streamName,
                eventSourcingProperties.streamReadPeriod,
                eventsChannel,
                kafkaConsumer,
                retryConfig,
                eventStreamListener,
                eventStreamsDispatcher
            )
        )

        if (existing != null) throw IllegalStateException("There is already stream $streamName for topic ${topicClass.simpleName}")

        return eventStreams[streamName] as KafkaConsumerEventStream<T>
    }

    fun destroy() {
        eventStreams.values.forEach {
            it.stopAndDestroy()
        }
    }

    fun maintenance(block: EventStreamListener.() -> Unit) {
        block(eventStreamListener)
    }

    fun streamsInfo() = eventStreams.map { (_, stream) ->
        StreamInfo(
            stream.streamName
        )
    }.toList()

    fun getStreamByName(name: String) = eventStreams[name]

    data class StreamInfo(
        val streamName: String
    )
}