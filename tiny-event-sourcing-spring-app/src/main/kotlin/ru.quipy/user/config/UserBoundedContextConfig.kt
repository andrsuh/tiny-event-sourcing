package ru.quipy.user.config

import ru.quipy.user.logic.UserAggregateState
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.user.api.UserAggregate
import java.util.UUID

@Configuration
class UserBoundedContextConfig {

    @Autowired
    private lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory

    @Bean
    fun userEsService(): EventSourcingService<UUID, UserAggregate, UserAggregateState> =
        eventSourcingServiceFactory.create()
}