package ru.quipy.catalog.config

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.catalog.api.CatalogAggregate
import ru.quipy.catalog.logic.CatalogAggregateState
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import java.util.UUID

@Configuration
class CatalogBoundedContextConfig {
    @Autowired
    private lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory

    @Bean
    fun catalogItemESService(): EventSourcingService<UUID, CatalogAggregate, CatalogAggregateState> = eventSourcingServiceFactory.create()
}