package ru.quipy.autoconfigure

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import ru.quipy.MongoTemplateOngoingGroupStorage
import ru.quipy.database.OngoingGroupStorage
import ru.quipy.storage.MongoClientOngoingGroupStorage
import ru.quipy.storage.converter.JacksonMongoEntityConverter
import ru.quipy.storage.factory.MongoClientFactory

@Configuration
class StorageAutoConfiguration {

    @Bean
    @ConditionalOnBean(MongoTemplate::class)
    @ConditionalOnMissingBean
    fun mongoTemplateStorage(): OngoingGroupStorage = MongoTemplateOngoingGroupStorage()

    @Bean
    @ConditionalOnBean(MongoClientFactory::class)
    @ConditionalOnMissingBean
    fun mongoClientStorage(databaseFactory: MongoClientFactory): OngoingGroupStorage {
        return MongoClientOngoingGroupStorage(JacksonMongoEntityConverter(), databaseFactory)
    }

    @Bean
    @ConditionalOnBean(MongoTransactionManager::class)
    @ConditionalOnMissingBean
    fun transactionManager(dbFactory: MongoDatabaseFactory): MongoTransactionManager {
        return MongoTransactionManager(dbFactory)
    }
}