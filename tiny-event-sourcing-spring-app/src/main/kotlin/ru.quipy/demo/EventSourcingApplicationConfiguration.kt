package ru.quipy.demo

import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.streams.AggregateSubscriptionsManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.demo.domain.UserAggregate
import ru.quipy.streams.AggregateEventStreamManager
import javax.annotation.PostConstruct

/**
 * Here you can define [EventSourcingService]s for your aggregates and initialize subscribers.
 */
@Configuration
class EventSourcingApplicationConfiguration {

    private val logger = LoggerFactory.getLogger(EventSourcingApplicationConfiguration::class.java)

    @Autowired
    private lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var userEventsSubscriber: AnnotationBasedUserEventsSubscriber

    @Autowired
    private lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory

    @Autowired
    private lateinit var eventStreamManager: AggregateEventStreamManager

    @PostConstruct
    fun init() {
        // auto scan enabled see event.sourcing.auto-scan-enabled property
//        aggregateRegistry.register(ProjectAggregate::class) {
//            registerEvent(TagCreatedEvent::class)
//            registerEvent(TaskCreatedEvent::class)
//            registerEvent(TagAssignedToTaskEvent::class)
//        }

        // says to [AggregateSubscriptionsManager] to scan the class and subscribe instance to aggregate event stream
        subscriptionsManager.subscribe<UserAggregate>(userEventsSubscriber)

        // not necessary. Just show the possible functionality. This way you can send metrics for example.
        eventStreamManager.maintenance {
            onRecordHandledSuccessfully { streamName, eventName ->
                logger.info("Stream $streamName successfully processed record of $eventName")
            }

            onBatchRead { streamName, batchSize ->
                logger.info("Stream $streamName read batch size: $batchSize")
            }
        }
    }

    @Bean
    fun userEventSourcingService() = eventSourcingServiceFactory.getOrCreateService(UserAggregate::class)

}