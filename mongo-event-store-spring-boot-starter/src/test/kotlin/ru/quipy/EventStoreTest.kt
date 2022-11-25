package ru.quipy

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import ru.quipy.autoconfigure.EventStoreAutoConfiguration
import ru.quipy.database.EventStore
import ru.quipy.eventstore.factory.MongoClientFactory
import ru.quipy.eventstore.factory.MongoClientFactoryImpl

@SpringBootTest(classes = [EventStoreAutoConfiguration::class])
@Import(EventStoreTestConfiguration::class)
class EventStoreTest {

    @Autowired
    private lateinit var eventStore : EventStore

    @Test
    fun updateSerialTest() {

    }
}