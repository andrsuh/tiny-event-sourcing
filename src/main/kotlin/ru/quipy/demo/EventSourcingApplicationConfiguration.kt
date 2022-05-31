package ru.quipy.demo

import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.streams.AggregateSubscriptionsManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.annotation.PostConstruct

@Configuration
class EventSourcingApplicationConfiguration {

    @Autowired
    private lateinit var aggregateRegistry: AggregateRegistry

    @Autowired
    private lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var projectEventSubscriber: ProjectEventSubscriber

    @Autowired
    private lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory

    @PostConstruct
    fun init() {
        aggregateRegistry.register(ProjectAggregate::class) {
            registerEvent(TagCreatedEvent::class)
            registerEvent(TaskCreatedEvent::class)
            registerEvent(TagAssignedToTaskEvent::class)
        }

        subscriptionsManager.subscribe<ProjectAggregate>(projectEventSubscriber)
    }

    @Bean
    fun demoESService() = eventSourcingServiceFactory.getOrCreateService(ProjectAggregate::class)

}