package ru.quipy.updateSerial

import com.mongodb.client.MongoClient
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import ru.quipy.MongoTemplateEventStore
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.database.EventStore
import ru.quipy.eventstore.MongoClientEventStore
import ru.quipy.eventstore.converter.JacksonMongoEntityConverter
import ru.quipy.eventstore.factory.MongoClientFactoryImpl
import ru.quipy.mapper.JsonEventMapper
import java.util.*

@Configuration
class UpdateSerialTestConfiguration {
    companion object {
        const val DATABASE_NAME = "tiny-es-test"
    }
    @Bean
    fun service(
        eventSourcingServiceFactory: EventSourcingServiceFactory
    ) : EventSourcingService<UUID, TestAggregate, TestAggregateState> = eventSourcingServiceFactory.create()

    @Bean
    @Primary
    fun mongoTemplateEventStore() : EventStore = MongoTemplateEventStore()

    @Bean
    fun mongoClientEventStore(mongoClient: MongoClient) : EventStore = MongoClientEventStore(
        JacksonMongoEntityConverter(),
        MongoClientFactoryImpl(mongoClient, DATABASE_NAME)
    )

    @Bean
    @Qualifier("service-with-mongo-template")
    fun serviceWithMongoTemplateEventStore(
        eventSourcingProperties: EventSourcingProperties,
        aggregateRegistry: AggregateRegistry,
        eventMapper: JsonEventMapper,
    ) : EventSourcingService<UUID, TestAggregate, TestAggregateState> {
        return EventSourcingServiceFactory(
            aggregateRegistry, eventMapper, mongoTemplateEventStore(), eventSourcingProperties
        ).create()
    }

    @Bean
    @Qualifier("service-with-mongo-client")
    fun serviceWithMongoClientEventStore(
        eventSourcingProperties: EventSourcingProperties,
        aggregateRegistry: AggregateRegistry,
        eventMapper: JsonEventMapper,
        mongoClient: MongoClient
    ) : EventSourcingService<UUID, TestAggregate, TestAggregateState> {
        return EventSourcingServiceFactory(
            aggregateRegistry, eventMapper, mongoClientEventStore(mongoClient), eventSourcingProperties
        ).create()
    }

}