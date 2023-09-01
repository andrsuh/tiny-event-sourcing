package ru.quipy.config

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.api.external.ProjectTopic
import ru.quipy.api.internal.ProjectAggregate
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.kafka.streams.TopicEventStreamManager
import ru.quipy.kafka.streams.TopicSubscriptionsManager
import ru.quipy.logic.ProjectAggregateState
import ru.quipy.streams.AggregateEventStreamManager
import java.util.*
import javax.annotation.PostConstruct

@Configuration
class EventSourcingLibConfiguration {

    private val logger = LoggerFactory.getLogger(EventSourcingLibConfiguration::class.java)

    @Autowired
    private lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory

    @Bean
    fun projectEsService() = eventSourcingServiceFactory.create<UUID, ProjectAggregate, ProjectAggregateState>()

    @Autowired
    private lateinit var eventAggregateAStreamManager: AggregateEventStreamManager

    @PostConstruct
    fun initAggregateStreams() {
        eventAggregateAStreamManager.maintenance {
            onStreamLaunched { streamName ->
                logger.info("Stream $streamName successfully launched")
            }

            onRecordHandledSuccessfully { streamName, eventName ->
                logger.info("Stream $streamName successfully processed internal event record of $eventName data base")
            }

            onBatchRead { streamName, batchSize ->
                logger.info("Stream $streamName read batch size: $batchSize")
            }
        }
    }

    @Autowired
    private lateinit var topicProjectEventStreamManager: TopicEventStreamManager


    @PostConstruct
    fun initTopicStreams() {
        topicProjectEventStreamManager.maintenance {
            onStreamLaunched { streamName ->
                logger.info("Stream $streamName successfully launched")
            }

            onRecordHandledSuccessfully { streamName, eventName ->
                logger.info("Stream $streamName successfully processed internal event record of $eventName data base")
            }

            onBatchRead { streamName, batchSize ->
                logger.info("Stream $streamName read batch size: $batchSize")
            }
        }
    }

    @Autowired
    private lateinit var topicProjectSubscriptionsManager: TopicSubscriptionsManager

    @Bean
    fun kafkaAggregateAProducer() = topicProjectSubscriptionsManager.createKafkaProducerSubscriber(
        ProjectAggregate::class,
        ProjectTopic::class,
        "aggregate-a-topic-a-subscriber"
    )
}