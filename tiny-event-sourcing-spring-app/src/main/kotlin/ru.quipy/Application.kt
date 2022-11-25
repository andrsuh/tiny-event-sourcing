package ru.quipy

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import ru.quipy.eventstore.factory.MongoClientFactory
import ru.quipy.eventstore.factory.MongoClientFactoryImpl

@SpringBootApplication
class Application {
    @Bean
    fun mongoClientFactory(): MongoClientFactory {
        return MongoClientFactoryImpl("tiny-es")
    }
}

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}
