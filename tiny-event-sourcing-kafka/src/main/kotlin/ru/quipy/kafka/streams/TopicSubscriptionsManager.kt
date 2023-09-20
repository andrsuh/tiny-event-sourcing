package ru.quipy.kafka.streams

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import ru.quipy.core.AggregateRegistry
import ru.quipy.domain.Aggregate
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.Topic
import ru.quipy.kafka.core.KafkaProperties
import ru.quipy.kafka.core.OngoingGroupManager
import ru.quipy.kafka.registry.DomainGroupRegistry
import ru.quipy.kafka.registry.TopicRegistry
import ru.quipy.mapper.EventMapper
import ru.quipy.mapper.ExternalEventMapper
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.StoppableAndDestructible
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import kotlin.reflect.KClass

/**
 * Creates [KafkaProducerSubscriber] and [KafkaConsumerSubscriber], holds them and allows to destroy them all.
 * Using this class is a preferable way to create and initialize subscribers.
 */
class TopicSubscriptionsManager(
    private val aggregateEventsStreamManager: AggregateEventStreamManager,
    private val topicEventsStreamManager: TopicEventStreamManager,
    private val aggregateRegistry: AggregateRegistry,
    private val topicRegistry: TopicRegistry,
    private val eventMapper: EventMapper,
    private val externalEventMapper: ExternalEventMapper,
    private val kafkaProperties: KafkaProperties,
    private val groupRegistry: DomainGroupRegistry,
    private val kafkaTopicCreator: KafkaTopicCreator,
    private val ongoingGroupManager: OngoingGroupManager
) {
    private val logger = LoggerFactory.getLogger(TopicSubscriptionsManager::class.java)

    private val subscribers: MutableSet<StoppableAndDestructible> = mutableSetOf()

    private val objectMapper = ObjectMapper()

    fun <A : Aggregate, T : Topic> createKafkaProducerSubscriber(
        aggregateClass: KClass<A>,
        topicEntityClass: KClass<T>,
        subscriberName: String,
        retryConf: RetryConf = RetryConf(3, RetryFailedStrategy.SKIP_EVENT)
    ): KafkaProducerSubscriber<A, T> {
        logger.info("Start creating subscription to aggregate ${aggregateClass.simpleName}, on topic ${topicEntityClass.simpleName}, subscriber name $subscriberName")

        val internalEventInfo = aggregateRegistry.getEventInfo(aggregateClass)
            ?: throw IllegalArgumentException("Couldn't find aggregate class ${aggregateClass.simpleName} in registry")

        val externalEventInfo = topicRegistry.getExternalEventInfo(topicEntityClass)
            ?: throw IllegalArgumentException("Couldn't find topic class ${topicEntityClass.simpleName} in registry")

        val stream = aggregateEventsStreamManager.createEventStream(
            "$subscriberName-stream",
            aggregateClass,
            retryConf
        )

        val topicName = topicRegistry.basicTopicInfo(topicEntityClass)?.topicName.toString()

        val topicConfig = TopicConfig(
            kafkaProperties.bootstrapServers!!,
            topicName,
            kafkaProperties.partitions,
            kafkaProperties.replicationFactor
        )

        kafkaTopicCreator.createTopicIfNotExists(topicConfig)

        val kafkaProducer = KafkaEventProducer<T>(topicName, kafkaProperties, objectMapper)

        val subscriber = KafkaProducerSubscriber(
            stream,
            kafkaProducer,
            groupRegistry,
            ongoingGroupManager,
            eventMapper,
            internalEventInfo::getEventTypeByName
        )

        if (subscribers.contains(subscriber)) {
            throw IllegalArgumentException("Subscriber with name aggregate $aggregateClass and topic $topicEntityClass already exists")
        }

        return subscriber.also { subscribers.add(it) }
    }

    fun <T : Topic> createKafkaConsumerSubscriber(
        topicEntityClass: KClass<T>,
        subscriberName: String,
        retryConf: RetryConf = RetryConf(3, RetryFailedStrategy.SKIP_EVENT),
        handlersBlock: ExternalEventHandlersRegistrar<T>.() -> Unit
    ): KafkaConsumerSubscriber<T> {
        val externalEventInfo = topicRegistry.getExternalEventInfo(topicEntityClass)
            ?: throw IllegalArgumentException("Couldn't find topic class ${topicEntityClass.simpleName} in registry")

        val subscriptionBuilder =
            topicEventsStreamManager.createKafkaConsumerStream(
                "$subscriberName-stream",
                topicEntityClass,
                retryConf
            )
                .toSubscriptionBuilder(externalEventMapper, externalEventInfo::getExternalEventTypeByName)

        handlersBlock.invoke(ExternalEventHandlersRegistrar(subscriptionBuilder))

        return subscriptionBuilder.subscribe().also {
            subscribers.add(it)
        }
    }

    fun destroy() {
        subscribers.forEach {
            it.stopAndDestroy()
        }
    }

    class ExternalEventHandlersRegistrar<T : Topic>(
        private val subscriptionBuilder: KafkaConsumerSubscriber.EventStreamSubscriptionBuilder<T>
    ) {
        fun <E : ExternalEvent<T>> `when`(
            eventType: KClass<E>,
            eventHandler: suspend (E) -> Unit
        ) {
            subscriptionBuilder.`when`(eventType, eventHandler)
        }
    }
}