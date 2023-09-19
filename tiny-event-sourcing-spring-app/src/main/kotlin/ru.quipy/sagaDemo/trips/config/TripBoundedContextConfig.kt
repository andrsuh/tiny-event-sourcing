package ru.quipy.sagaDemo.trips.config

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.sagaDemo.trips.api.TripAggregate
import ru.quipy.sagaDemo.trips.logic.Trip
import java.util.*

@Configuration
class TripBoundedContextConfig {

    @Autowired
    private lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory

    @Bean
    fun tripEsService(): EventSourcingService<UUID, TripAggregate, Trip> =
        eventSourcingServiceFactory.create()
}