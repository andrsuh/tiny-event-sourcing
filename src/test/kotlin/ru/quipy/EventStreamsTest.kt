package ru.quipy

import com.fasterxml.jackson.databind.ObjectMapper
import org.awaitility.Awaitility
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.ConfigProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.database.EventStoreDbOperations
import ru.quipy.demo.*
import ru.quipy.mapper.EventMapper
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.streams.AggregateSubscriber
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.SubscribeEvent
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@SpringBootTest
class EventStreamsTest {
    companion object {
        const val testId = "1"
    }

    @Autowired
    private lateinit var dbOperations: EventStoreDbOperations

    @Autowired
    private lateinit var subscriptionsManager: AggregateSubscriptionsManager

    private lateinit var aggregateRegistry: AggregateRegistry

    private lateinit var config: ConfigProperties

    private lateinit var eventMapper: EventMapper

    private lateinit var esServiceFactory: EventSourcingServiceFactory

    private lateinit var demoESService: EventSourcingService<ProjectAggregate>

    @Mock
    private lateinit var testService: TestService

    private lateinit var tested: TestDemoProjectSubscriber

    @BeforeEach
    fun init() {
        config = ConfigProperties().also {
            it.snapshotFrequency = Int.MAX_VALUE
        }

        aggregateRegistry = AggregateRegistry().also {
            it.register(ProjectAggregate::class) {
                registerEvent(TagCreatedEvent::class)
                registerEvent(TaskCreatedEvent::class)
                registerEvent(TagAssignedToTaskEvent::class)
            }
        }

        eventMapper = JsonEventMapper(ObjectMapper())
        esServiceFactory = EventSourcingServiceFactory(
            aggregateRegistry, eventMapper, dbOperations, config
        )

        demoESService = esServiceFactory.getOrCreateService(ProjectAggregate::class)

        tested = TestDemoProjectSubscriber(subscriptionsManager).also {
            it.init()
        }
    }

    @Test
    fun successFlow() {
        val id = UUID.randomUUID().toString()

        demoESService.update(id) {
            it.addTask("task!")
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until {
            tested.testStats.success.get() == 1
        }
    }

    @Test
    fun errorFlow() {
        val id = UUID.randomUUID().toString()

        Mockito.`when`(tested.someMockedService.act())
            .thenThrow(IllegalArgumentException("12345"))

        demoESService.update(id) {
            it.addTask("task!")
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until {
            tested.testStats.failure.get() == 1
        }
    }


    @AggregateSubscriber(
        aggregateClass = ProjectAggregate::class,
        subscriberName = "test-subs-stream"
    )
    class TestDemoProjectSubscriber(
        val subscriptionsManager: AggregateSubscriptionsManager
    ) {
        val someMockedService: TestService = Mockito.mock(TestService::class.java)

        val testStats = TestStats()

        fun init() {
            subscriptionsManager.subscribe<ProjectAggregate>(this)
        }

        @SubscribeEvent
        fun taskCreatedSubscriber(event: TaskCreatedEvent) {
            try {
                someMockedService.act()
                testStats.success.incrementAndGet()
            } catch (e: Exception) {
                testStats.failure.incrementAndGet()
                throw e
            }
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
