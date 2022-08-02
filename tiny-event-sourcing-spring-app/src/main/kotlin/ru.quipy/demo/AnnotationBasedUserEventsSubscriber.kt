package ru.quipy.demo

import org.slf4j.*
import org.springframework.stereotype.Service
import ru.quipy.demo.UserAggregate
import ru.quipy.demo.UserCreatedEvent
import ru.quipy.streams.annotation.AggregateSubscriber
import ru.quipy.streams.annotation.SubscribeEvent

@Service
@AggregateSubscriber(aggregateClass = UserAggregate::class, subscriberName = "demo-user-stream")
class AnnotationBasedUserEventsSubscriber {
    val logger: Logger = LoggerFactory.getLogger(AnnotationBasedUserEventsSubscriber::class.java)
    @SubscribeEvent
    fun userCreatedSubscriber(event: UserCreatedEvent) {
        logger.info("User created {}", event.userName)
    }
}