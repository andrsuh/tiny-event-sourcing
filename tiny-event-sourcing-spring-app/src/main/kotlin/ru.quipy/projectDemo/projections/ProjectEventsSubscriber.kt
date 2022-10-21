package ru.quipy.projectDemo.projections

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.projectDemo.api.ProjectAggregate
import ru.quipy.projectDemo.api.TagAssignedToTaskEvent
import ru.quipy.projectDemo.api.TagCreatedEvent
import ru.quipy.projectDemo.api.TaskCreatedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import javax.annotation.PostConstruct

@Service
class ProjectEventsSubscriber {

    val logger: Logger = LoggerFactory.getLogger(ProjectEventsSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(ProjectAggregate::class, "some-meaningful-name") {

            `when`(TaskCreatedEvent::class) { event ->
                logger.info("Task created: {}", event.taskName)
            }

            `when`(TagCreatedEvent::class) { event ->
                logger.info("Tag created: {}", event.tagName)
            }

            `when`(TagAssignedToTaskEvent::class) { event ->
                logger.info("Tag {} assigned to task {}: ", event.tagId, event.taskId)
            }
        }
    }
}