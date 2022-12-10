package ru.quipy.autoconfigure

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import ru.quipy.MongoTemplateEventStore
import ru.quipy.database.EventStore
import ru.quipy.eventstore.MongoClientEventStore
import ru.quipy.eventstore.converter.JacksonMongoEntityConverter
import ru.quipy.eventstore.factory.MongoClientFactory

@Configuration
class EventStoreAutoConfiguration {

    @Bean
    @ConditionalOnBean(MongoTemplate::class)
    @ConditionalOnMissingBean
    fun mongoTemplateEventStore(): EventStore = MongoTemplateEventStore()

    @Bean
    @ConditionalOnBean(MongoClientFactory::class)
    @ConditionalOnMissingBean
    fun mongoClientEventStore(databaseFactory: MongoClientFactory): EventStore {
        return MongoClientEventStore(JacksonMongoEntityConverter(), databaseFactory)
    }

    @Bean
    @ConditionalOnBean(MongoTransactionManager::class)
    @ConditionalOnMissingBean
    fun transactionManager(dbFactory: MongoDatabaseFactory): MongoTransactionManager {
        return MongoTransactionManager(dbFactory)
    }
}