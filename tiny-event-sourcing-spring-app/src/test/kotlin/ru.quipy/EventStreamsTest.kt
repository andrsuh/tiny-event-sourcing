package ru.quipy

import org.awaitility.Awaitility
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import ru.quipy.core.EventSourcingService
import ru.quipy.demo.ProjectAggregate
import ru.quipy.demo.TaskCreatedEvent
import ru.quipy.demo.addTask
import ru.quipy.streams.AggregateSubscriber
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.SubscribeEvent
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.PostConstruct

@SpringBootTest
@Import(SubscriptionConfig::class)
class EventStreamsTest {
    companion object {
        const val testId = "1"
    }

    @Autowired
    private lateinit var demoESService: EventSourcingService<ProjectAggregate>

    @Autowired
    lateinit var tested: TestDemoProjectSubscriber

    @Autowired
    lateinit var mongoTemplate: MongoTemplate

    fun cleanDatabase() {
        mongoTemplate.remove(Query.query(Criteria.where("aggregateId").`is`(testId)), "aggregate-project")
        mongoTemplate.remove(Query.query(Criteria.where("_id").`is`(testId)), "snapshots")
    }

    @BeforeEach
    fun init() {
        cleanDatabase()
    }

    @Test
    fun successFlow() {
        Mockito.doNothing().`when`(tested.someMockedService).act()

        demoESService.update(testId) {
            it.addTask("task!")
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until {
            tested.testStats.success.get() == 1
        }
    }

    @Test
    fun errorFlow() {
        Mockito.`when`(tested.someMockedService.act())
            .thenThrow(IllegalArgumentException("12345"))

        demoESService.update(testId) {
            it.addTask("task!")
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until {
            tested.testStats.failure.get() > 0
        }
    }

    class TestStats {
        val success = AtomicInteger()
        val failure = AtomicInteger()
    }

    open class TestService {
        open fun act() = Unit
    }
}

@TestConfiguration
@Import(TestProjectSubscriberConfig::class)
open class SubscriptionConfig {

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    lateinit var subscriber: TestDemoProjectSubscriber

    @PostConstruct
    fun init() {
        subscriptionsManager.subscribe<ProjectAggregate>(subscriber)
    }
}


@TestConfiguration
open class TestProjectSubscriberConfig {

    @Bean
    fun testDemoProjectSubscriber() = TestDemoProjectSubscriber()
}

@AggregateSubscriber(
    aggregateClass = ProjectAggregate::class,
    subscriberName = "test-subs-stream"
)
class TestDemoProjectSubscriber {
    val someMockedService: EventStreamsTest.TestService = Mockito.mock(EventStreamsTest.TestService::class.java)

    val testStats = EventStreamsTest.TestStats()

    @SubscribeEvent
    fun taskCreatedSubscriber(event: TaskCreatedEvent) {
        try {
            someMockedService.act()
            if (event.aggregateId == EventStreamsTest.testId)
                testStats.success.incrementAndGet()
        } catch (e: Exception) {
            if (event.aggregateId == EventStreamsTest.testId)
                testStats.failure.incrementAndGet()
            throw e
        }
    }
}
