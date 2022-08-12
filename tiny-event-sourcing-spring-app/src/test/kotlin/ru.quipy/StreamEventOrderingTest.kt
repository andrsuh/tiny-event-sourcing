package ru.quipy

import org.awaitility.kotlin.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import ru.quipy.core.EventSourcingService
import ru.quipy.demo.domain.UserAddedAddressEvent
import ru.quipy.demo.domain.UserAggregate
import ru.quipy.demo.domain.addAddressCommand
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.concurrent.TimeUnit

@SpringBootTest(properties = ["event.sourcing.stream-batch-size=3"])
class StreamEventOrderingTest {
    companion object {
        const val testId = "3"
    }

    @Autowired
    private lateinit var esService: EventSourcingService<UserAggregate>

    @Autowired
    private lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    lateinit var mongoTemplate: MongoTemplate

    fun cleanDatabase() {
        mongoTemplate.remove(Query.query(Criteria.where("aggregateId").`is`(testId)), "aggregate-user")
        mongoTemplate.remove(Query.query(Criteria.where("_id").`is`(testId)), "snapshots")
    }

    private val sb = StringBuilder()

    @BeforeEach
    fun init() {
        cleanDatabase()
    }

    @Test
    fun testEventOrder() {
        esService.update(testId) {
            it.addAddressCommand("1")
        }
        esService.update(testId) {
            it.addAddressCommand("2")
        }
        esService.update(testId) {
            it.addAddressCommand("3")
        }
        esService.update(testId) {
            it.addAddressCommand("4")
        }
        esService.update(testId) {
            it.addAddressCommand("5")
        }
        esService.update(testId) {
            it.addAddressCommand("6")
        }


        subscriptionsManager.createSubscriber(UserAggregate::class, "StreamEventOrderingTest") {
            `when`(UserAddedAddressEvent::class) { event ->
                sb.append(event.address).also {
                    println(sb.toString())
                }
            }
        }

        await.atMost(10, TimeUnit.MINUTES).until {
            sb.toString() == "123456"
        }
    }
}