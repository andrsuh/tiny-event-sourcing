package ru.quipy

import org.awaitility.Awaitility
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.atMostOnce
import org.mockito.kotlin.any
import org.mockito.kotlin.argWhere
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import ru.quipy.core.EventSourcingService
import ru.quipy.projectDemo.addTask
import ru.quipy.projectDemo.api.ProjectAggregate
import ru.quipy.projectDemo.api.TaskCreatedEvent
import ru.quipy.projectDemo.create
import ru.quipy.projectDemo.logic.ProjectAggregateState
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.AggregateSubscriber
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy.SKIP_EVENT
import ru.quipy.streams.annotation.SubscribeEvent
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.PostConstruct

@SpringBootTest
@ActiveProfiles("test")
@Import(SubscriptionConfig::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class EventStreamsTest {
    companion object {
        const val testId = "2"
    }

    @Autowired
    private lateinit var demoESService: EventSourcingService<String, ProjectAggregate, ProjectAggregateState>

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
        demoESService.create { project ->
            project.create(testId)
        }
    }

    @Test
    fun successFlow() {
        Mockito.doNothing().`when`(tested.someMockedService).act(any())

        val succeededBefore = tested.testStats.success.get()
        demoESService.update(testId) { project ->
            project.addTask("task!")
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until {
            tested.testStats.success.get() == succeededBefore + 1
        }
    }

    @Test
    fun errorFlow() {
        Mockito.`when`(tested.someMockedService.act(any()))
            .thenThrow(IllegalArgumentException("12345"))

        val failuresBefore = tested.testStats.failure.get()
        demoESService.update(testId) {
            it.addTask("task!")
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until {
            tested.testStats.failure.get() == failuresBefore + 3
        }
    }

    @Test
    fun errorFlowRetry3TimesThenSkip() {
        Mockito.`when`(tested.someMockedService.act(any()))
            .thenThrow(IllegalArgumentException("12345"))

        val failuresBefore = tested.testStats.failure.get()
        demoESService.update(testId) {
            it.addTask("task!")
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until {
            tested.testStats.failure.get() == failuresBefore + 3
        }

        Mockito.doNothing().`when`(tested.someMockedService).act(any())

        val succeededBefore = tested.testStats.success.get()
        val successEvent = demoESService.update(testId) {
            it.addTask("task!")
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until {
            tested.testStats.success.get() == succeededBefore + 1
        }

        Mockito.verify(tested.someMockedService, atMostOnce()).act(argWhere { it.id == successEvent.id })
    }

    class TestStats {
        val success = AtomicInteger()
        val failure = AtomicInteger()
    }

    open class TestService {
        open fun act(event: TaskCreatedEvent) = Unit
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

@Suppress("unused")
@AggregateSubscriber(
    aggregateClass = ProjectAggregate::class,
    subscriberName = "test-subscription-stream",
    retry = RetryConf(3, SKIP_EVENT)
)
class TestDemoProjectSubscriber {
    val someMockedService: EventStreamsTest.TestService = Mockito.mock(EventStreamsTest.TestService::class.java)

    val testStats = EventStreamsTest.TestStats()

    @SubscribeEvent
    fun taskCreatedSubscriber(event: TaskCreatedEvent) {
        try {
            someMockedService.act(event)
            if (event.projectId == EventStreamsTest.testId)
                testStats.success.incrementAndGet()
        } catch (e: Exception) {
            if (event.projectId == EventStreamsTest.testId)
                testStats.failure.incrementAndGet()
            throw e
        }
    }
}
