package ru.quipy.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.api.external.ProjectTopic
import ru.quipy.api.external.TaskAndTagCreatedToExternalEventMapper
import ru.quipy.api.internal.ProjectAggregate
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.kafka.registry.ExternalEventMapperRegistry
import ru.quipy.kafka.streams.TopicEventStreamManager
import ru.quipy.kafka.streams.TopicSubscriptionsManager
import ru.quipy.logic.ProjectAggregateState
import ru.quipy.streams.AggregateEventStreamManager
import java.util.*
import javax.annotation.PostConstruct

@Configuration
class EventSourcingLibConfiguration(
    private val eventSourcingServiceFactory: EventSourcingServiceFactory,
    private val eventAggregateAStreamManager: AggregateEventStreamManager,
    private val topicProjectEventStreamManager: TopicEventStreamManager,
    private val topicProjectSubscriptionsManager: TopicSubscriptionsManager,
    private val externalEventMapperRegistry: ExternalEventMapperRegistry
) {

    private val logger = LoggerFactory.getLogger(EventSourcingLibConfiguration::class.java)

    @Bean
    fun projectEsService() = eventSourcingServiceFactory.create<UUID, ProjectAggregate, ProjectAggregateState>()

    @PostConstruct
    fun initMappers() {
//        externalEventMapperRegistry.injectEventMappers(listOf(ProjectCreatedToExternalEventMapper::class))
        externalEventMapperRegistry.injectGroupMappers(listOf(TaskAndTagCreatedToExternalEventMapper::class))
    }

    @PostConstruct
    fun initAggregateStreams() {
        eventAggregateAStreamManager.maintenance {
            onStreamLaunched { streamName ->
                logger.info("Internal event stream $streamName successfully launched")
            }

            onRecordHandledSuccessfully { streamName, eventName ->
                logger.info("Internal event stream $streamName successfully processed internal event record of $eventName data base")
            }

            onBatchRead { streamName, batchSize ->
                logger.info("Internal event stream $streamName read batch size: $batchSize")
            }
        }
    }

    @PostConstruct
    fun initTopicStreams() {
        topicProjectEventStreamManager.maintenance {
            onStreamLaunched { streamName ->
                logger.info("External event stream $streamName successfully launched")
            }

            onRecordHandledSuccessfully { streamName, eventName ->
                logger.info("External event stream $streamName successfully processed internal event record of $eventName data base")
            }

            onBatchRead { streamName, batchSize ->
                logger.info("External event stream $streamName poll batch size: $batchSize")
            }
        }
    }

    @Bean
    fun kafkaAggregateAProducer() = topicProjectSubscriptionsManager.createKafkaProducerSubscriber(
        ProjectAggregate::class,
        ProjectTopic::class,
        "aggregate-a-topic-a-subscriber"
    )
}
