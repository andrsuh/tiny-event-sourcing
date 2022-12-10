package ru.quipy.updateSerial

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.EventRecord
import ru.quipy.updateSerial.UpdateSerialTestConfiguration.Companion.DATABASE_NAME
import java.util.*


@SpringBootTest(
    classes = [UpdateSerialTestConfiguration::class],
    properties = [
        "spring.data.mongodb.host=localhost",
        "spring.data.mongodb.port=27018",
        "spring.data.mongodb.database=".plus(DATABASE_NAME),
        "spring.mongodb.embedded.version=4.0.12",
        "event.sourcing.auto-scan-enabled=true",
        "event.sourcing.scan-package=ru.quipy.updateSerial",
        "event.sourcing.spin-lock-max-attempts=10000",
        "event.sourcing.snapshot-frequency=10000"
    ],
)
@EnableAutoConfiguration
class UpdateSerialTest {

    companion object {
        private val testAggregateId = UUID.randomUUID()

        const val CREATED_EVENT_NAME = "SERIAL_TEST_CREATED_EVENT"
        const val TEST_EVENT_NAME = "SERIAL_TEST_EVENT"
        const val TEST_TABLE_NAME = "update-serial-test"

        const val BATCH_SIZE = 5
        const val CONCURRENT_TASKS = 5
        const val ITERATIONS_PER_TASK = 10
    }

    //
    @Autowired
    private lateinit var mongoTemplate: MongoTemplate

    @Autowired
    @Qualifier("service-with-mongo-client")
    private lateinit var serviceWithMongoClientEventStore: EventSourcingService<UUID, TestAggregate, TestAggregateState>

    @Autowired
    @Qualifier("service-with-mongo-template")
    private lateinit var serviceWithMongoTemplateEventStore: EventSourcingService<UUID, TestAggregate, TestAggregateState>

    @Autowired
    private lateinit var mapper: ObjectMapper

    @Autowired
    private lateinit var properties: EventSourcingProperties


    @AfterEach
    @BeforeEach
    fun cleanDatabase() {
        mongoTemplate.dropCollection(TEST_TABLE_NAME)
        mongoTemplate.dropCollection(properties.snapshotTableName)
    }

    private fun generateEventRecordsBatch(size: Int, aggregate: TestAggregateState) =
        List(size) { aggregate.testUpdateSerial(it) }

    private fun getExpectedMask(): String {
        return List(ITERATIONS_PER_TASK * CONCURRENT_TASKS) {
            List(BATCH_SIZE) { it }
        }.flatten().joinToString("")
    }

    private fun getActualMask(): String {
        val query = Query()
            .addCriteria(Criteria.where("eventTitle").`is`(TEST_EVENT_NAME))
            .with(Sort.by("version").ascending())
        return mongoTemplate
            .find(query, EventRecord::class.java, TEST_TABLE_NAME)
            .joinToString("") {
                mapper.readValue(it.payload, TestUpdateSerialEvent::class.java).order.toString()
            }
    }

    fun testVersionOrder(service: EventSourcingService<UUID, TestAggregate, TestAggregateState>) {
        service.create {
            it.create(testAggregateId)
        }
        runBlocking {
            repeat(CONCURRENT_TASKS) {
                launch(Dispatchers.Default) {
                    repeat(ITERATIONS_PER_TASK) {
                        service.updateSerial(testAggregateId) {
                            generateEventRecordsBatch(BATCH_SIZE, it)
                        }
                    }
                }
            }
        }
        Assertions.assertEquals(
            getExpectedMask(),
            getActualMask()
        )
    }

    @Test
    fun mongoTemplateEventStoreTest() {
        testVersionOrder(serviceWithMongoTemplateEventStore)
    }

    @Test
    fun mongoClientEventStoreTest() {
        testVersionOrder(serviceWithMongoClientEventStore)
    }
}
