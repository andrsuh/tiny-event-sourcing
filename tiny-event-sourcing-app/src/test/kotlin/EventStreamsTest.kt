package ru.quipy

import org.awaitility.Awaitility
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.any
import org.mockito.Mockito.atMostOnce
import org.mockito.kotlin.argWhere
import ru.quipy.application.component.Component
import ru.quipy.core.EventSourcingProperties
import ru.quipy.projectDemo.addTask
import ru.quipy.projectDemo.api.ProjectAggregate
import ru.quipy.projectDemo.api.TaskCreatedEvent
import ru.quipy.projectDemo.create
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.AggregateSubscriber
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy.SKIP_EVENT
import ru.quipy.streams.annotation.SubscribeEvent
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class EventStreamsTest: BaseTest(testId) {
    companion object {
        const val testId = "EventStreamsTest"
        lateinit var tested: TestDemoProjectSubscriber

        @BeforeAll
        @JvmStatic
        fun configure() {
            configure(EventSourcingProperties())
            tested = TestProjectSubscriberConfig().testDemoProjectSubscriber()
            SubscriptionConfig(subscriptionsManager, tested).postConstruct()
        }
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

        Awaitility.await().atMost(10, TimeUnit.SECONDS).until {
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

        Awaitility.await().atMost(10, TimeUnit.SECONDS).until {
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

        Awaitility.await().atMost(10, TimeUnit.SECONDS).until {
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
        open fun act(event: TaskCreatedEvent?) = Unit
    }
}

open class SubscriptionConfig(
    var subscriptionsManager: AggregateSubscriptionsManager,
    var subscriber: TestDemoProjectSubscriber
) : Component {

    override fun postConstruct() {
        subscriptionsManager.subscribe<ProjectAggregate>(subscriber)
    }
}

open class TestProjectSubscriberConfig {
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
