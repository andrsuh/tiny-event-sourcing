package ru.quipy.spring

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.ConfigProperties
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.mapper.JsonEventMapper

@Configuration
open class SpringAppConfig {
    @Autowired
    lateinit var configProperties: ConfigProperties

    @Bean
    fun jsonObjectMapper() = jacksonObjectMapper()

    @Bean
    //@ConditionalOnMissingBean(EventMapper::class)
    fun eventMapper(jsonObjectMapper: ObjectMapper) = JsonEventMapper(jsonObjectMapper)

    @Bean
    //@ConditionalOnBean(MongoTemplate::class)
    fun eventStoreDbOperations() = MongoDbEventStoreDbOperations()

    @Bean
    fun aggregateRegistry() = AggregateRegistry()

    @Bean
    fun eventSourcingServiceFactory(
        aggregateRegistry: AggregateRegistry,
        eventMapper: JsonEventMapper,
        eventStoreDbOperations: EventStoreDbOperations
    ) = EventSourcingServiceFactory(
        aggregateRegistry, eventMapper, eventStoreDbOperations, configProperties
    )
}