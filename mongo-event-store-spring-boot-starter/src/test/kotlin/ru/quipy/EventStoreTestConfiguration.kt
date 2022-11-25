package ru.quipy

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import ru.quipy.autoconfigure.EventStoreAutoConfiguration
import ru.quipy.eventstore.factory.MongoClientFactory
import ru.quipy.eventstore.factory.MongoClientFactoryImpl

@TestConfiguration
class EventStoreTestConfiguration {
    @Bean
    fun mongoClientFactory(): MongoClientFactory {
        return MongoClientFactoryImpl("tiny-es")
    }
}