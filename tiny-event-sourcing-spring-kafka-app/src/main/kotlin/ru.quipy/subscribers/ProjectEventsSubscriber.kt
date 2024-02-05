package ru.quipy.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.api.external.ProjectCreatedExternalEvent
import ru.quipy.api.external.ProjectTopic
import ru.quipy.api.external.TaskAndTagCreatedExternalEvent
import ru.quipy.kafka.streams.TopicSubscriptionsManager
import javax.annotation.PostConstruct

@Service
class ProjectEventsSubscriber {

    val logger: Logger = LoggerFactory.getLogger(ProjectEventsSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: TopicSubscriptionsManager

    @PostConstruct
    fun init() {
        subscriptionsManager.createKafkaConsumerSubscriber(ProjectTopic::class, "project::some-meaningful-subscriber") {

            `when`(ProjectCreatedExternalEvent::class) { event ->
                logger.info("Project created: {}", event.title)
            }

            `when`(TaskAndTagCreatedExternalEvent::class) { event ->
                logger.info("Task {} and Tag {} created", event.taskName, event.tagName)
            }
        }
    }
}