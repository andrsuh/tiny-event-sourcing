package ru.quipy

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import ru.quipy.core.*
import ru.quipy.database.EventStore
import ru.quipy.mapper.EventMapper
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.projectDemo.api.*
import ru.quipy.projectDemo.create
import ru.quipy.projectDemo.createTag
import ru.quipy.projectDemo.logic.ProjectAggregateState
import ru.quipy.projectDemo.logic.tagAssignedApply
import ru.quipy.streams.ActiveEventStreamReaderManager
import ru.quipy.streams.AggregateEventStreamManager
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.EventStreamSubscriber
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ActiveStreamReaderTest {
    @Autowired
    private lateinit var eventStore: EventStore

    private val testId = "4"

    private val properties: EventSourcingProperties = EventSourcingProperties(
            maxActiveReaderInactivityPeriod = 5.seconds,
            recordReadIndexCommitPeriod = 2,
            eventReaderHealthCheckPeriodBase = 1.seconds)

    private val registry: AggregateRegistry = SeekingForSuitableClassesAggregateRegistry(BasicAggregateRegistry(), properties)

    private val eventMapper: EventMapper = JsonEventMapper(jacksonObjectMapper())

    private lateinit var eventStreamManager: AggregateEventStreamManager //= AggregateEventStreamManager(registry, eventStore, properties)

    private lateinit var subscriptionsManager: AggregateSubscriptionsManager// = AggregateSubscriptionsManager(eventStreamManager, registry, eventMapper)

    private lateinit var demoESService: EventSourcingService<String, ProjectAggregate, ProjectAggregateState>

    @BeforeEach
    fun init() {
        registry.register(ProjectAggregate::class, ProjectAggregateState::class) {
            registerStateTransition(ProjectCreatedEvent::class, ProjectAggregateState::projectCreatedApply)
            registerStateTransition(TagCreatedEvent::class, ProjectAggregateState::tagCreatedApply)
            registerStateTransition(TaskCreatedEvent::class, ProjectAggregateState::taskCreatedApply)
            registerStateTransition(TagAssignedToTaskEvent::class, ProjectAggregateState::tagAssignedApply)
        }

        eventStreamManager = AggregateEventStreamManager(registry, eventStore, properties)
        subscriptionsManager = AggregateSubscriptionsManager(eventStreamManager, registry, eventMapper)

        demoESService = EventSourcingService(
                aggregateClass = ProjectAggregate::class,
                aggregateRegistry = registry,
                eventStore = eventStore,
                eventMapper = eventMapper,
                eventSourcingProperties = properties)
    }

    @Test
    fun checkActiveReaderIntercepted() {
        val eventStreamManager1 = AggregateEventStreamManager(registry, eventStore, properties)
        val eventStreamManager2 = AggregateEventStreamManager(registry, eventStore, properties)

        val subscriptionManager1 = AggregateSubscriptionsManager(eventStreamManager1, registry, eventMapper)
        val subscriptionManager2 = AggregateSubscriptionsManager(eventStreamManager2, registry, eventMapper)

        val sb1 = StringBuilder()
        val sb2 = StringBuilder()

        val subscriber1: EventStreamSubscriber<ProjectAggregate> = subscriptionManager1.createSubscriber(ProjectAggregate::class, "ActiveStreamReaderTest") {
            `when`(TagCreatedEvent::class) { event ->
                sb1.append(event.tagName).also {
                    println(sb1.toString())
                }
            }
        }

        val subscriber2: EventStreamSubscriber<ProjectAggregate> = subscriptionManager2.createSubscriber(ProjectAggregate::class, "ActiveStreamReaderTest") {
            `when`(TagCreatedEvent::class) { event ->
                sb2.append(event.tagName).also {
                    println(sb2.toString())
                }
            }
        }

        demoESService.create {
            it.create(testId)
        }

        demoESService.update(testId) {
            it.createTag("A")
        }

        subscriber1.stopAndDestroy()

        demoESService.update(testId) {
            it.createTag("B")
        }

        demoESService.update(testId) {
            it.createTag("C")
        }

        val waitTimeSeconds = properties.maxActiveReaderInactivityPeriod.inWholeSeconds + properties.eventReaderHealthCheckPeriod.inWholeSeconds

        await.atMost(waitTimeSeconds, TimeUnit.SECONDS).until {
            sb2.toString() == "ABC"
        }
    }

    @Test
    fun hasActiveReader_ReturnsTrue() {
        val activeEventStreamReaderManager = ActiveEventStreamReaderManager(eventStore, properties)
        val reader = UUID.randomUUID().toString()

        activeEventStreamReaderManager.updateReaderState("test-stream", reader, readingIndex = 0L)

        val hasActiveReader = activeEventStreamReaderManager.hasActiveReader("test-stream")

        assertTrue(hasActiveReader)
    }

    @Test
    fun hasActiveReader_ReturnsFalse() {
        val activeEventStreamReaderManager = ActiveEventStreamReaderManager(eventStore, properties)
        val reader = UUID.randomUUID().toString()

        activeEventStreamReaderManager.updateReaderState("test-stream", reader, readingIndex = 0L)

        val waitTimeSeconds = properties.maxActiveReaderInactivityPeriod.inWholeSeconds + properties.eventReaderHealthCheckPeriod.inWholeSeconds

        await.atMost(waitTimeSeconds, TimeUnit.SECONDS).until {
            !activeEventStreamReaderManager.hasActiveReader("test-stream")
        }
    }
}