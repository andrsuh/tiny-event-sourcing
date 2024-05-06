package ru.quipy.projectDemo.config

import org.slf4j.LoggerFactory
import ru.quipy.application.component.Component
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.projectDemo.api.ProjectAggregate
import ru.quipy.projectDemo.logic.ProjectAggregateState
import ru.quipy.projectDemo.projections.AnnotationBasedProjectEventsSubscriber
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager

class ProjectDemoConfig(
    var subscriptionsManager: AggregateSubscriptionsManager,
    var projectEventSubscriber: AnnotationBasedProjectEventsSubscriber,
    var eventSourcingServiceFactory: EventSourcingServiceFactory,
    var eventStreamManager: AggregateEventStreamManager,
    var aggregateRegistry: AggregateRegistry
    ) : Component {

    private val logger = LoggerFactory.getLogger(ProjectDemoConfig::class.java)

    override fun postConstruct() {
        subscriptionsManager.subscribe<ProjectAggregate>(projectEventSubscriber)

        eventStreamManager.maintenance {
            onRecordHandledSuccessfully { streamName, eventName ->
                logger.info("Stream $streamName successfully processed record of $eventName")
            }

            onBatchRead { streamName, batchSize ->
                logger.info("Stream $streamName read batch size: $batchSize")
            }
        }
    }

    fun demoESService() = eventSourcingServiceFactory.create<String, ProjectAggregate, ProjectAggregateState>()
}