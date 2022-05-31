package ru.quipy.demo

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.streams.AggregateSubscriber
import ru.quipy.streams.SubscribeEvent

@Service
@AggregateSubscriber(
    aggregateClass = ProjectAggregate::class, subscriberName = "demo-subs-stream"
)
class ProjectEventSubscriber {

    val logger: Logger = LoggerFactory.getLogger(ProjectEventSubscriber::class.java)

    @SubscribeEvent
    fun taskCreatedSubscriber(event: TaskCreatedEvent) {
        logger.info("Got event {}", event.taskName)
    }
}