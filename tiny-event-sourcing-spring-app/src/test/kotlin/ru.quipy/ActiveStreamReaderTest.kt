package ru.quipy

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.*
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import ru.quipy.config.DockerPostgresDataSourceInitializer
import ru.quipy.core.*
import ru.quipy.database.EventStore
import ru.quipy.mapper.EventMapper
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.projectDemo.api.*
import ru.quipy.projectDemo.create
import ru.quipy.projectDemo.createTag
import ru.quipy.projectDemo.logic.ProjectAggregateState
import ru.quipy.projectDemo.logic.tagAssignedApply
import ru.quipy.streams.*
import java.time.Duration.*
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

@SpringBootTest
@ContextConfiguration(
    initializers = [DockerPostgresDataSourceInitializer::class])
@EnableAutoConfiguration
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ActiveStreamReaderTest: BaseTest("ActiveStreamReaderTest") {
    @Autowired
    private lateinit var eventStore: EventStore

    private val properties: EventSourcingProperties = EventSourcingProperties(
        streamBatchSize = 10,
        readerCommitPeriodMessages = 1,
        streamReadPeriod = 50, // todo sukhoa actually this is polling period id case batch is empty
        maxActiveReaderInactivityPeriod = 300.milliseconds,
        eventReaderHealthCheckPeriod = 50.milliseconds,
        snapshotFrequency = 10
    )
    private val streamNotFailingProbability = 0.95

    private val dispatcher =
        ThreadPoolExecutor(16, 16, Long.MAX_VALUE, TimeUnit.MILLISECONDS, LinkedBlockingQueue()).asCoroutineDispatcher()

    private val numberOfProjects = 50
    private val numberOfTagsPerProject = 200
    private val totalNumberOfEvents = numberOfProjects + (numberOfProjects * numberOfTagsPerProject)

    // 1. all is ok all the messages is processed by one stream
    // 2. all is ok, but there are some stream's failure. stream is successfully intercepted by another reader and all the events are processed
    // 3.
    // !!! Too big batch leads to many clashes as its cached in Buffered stream and cannot be rejected
    // Too small batch size increases the DB trips


    private val registry: AggregateRegistry =
        SeekingForSuitableClassesAggregateRegistry(BasicAggregateRegistry(), properties)

    private val eventMapper: EventMapper = JsonEventMapper(jacksonObjectMapper())

    private lateinit var eventStreamManager: AggregateEventStreamManager //= AggregateEventStreamManager(registry, eventStore, properties)

    private lateinit var eventStreamReaderManager: EventStreamReaderManager //= AggregateEventStreamManager(registry, eventStore, properties)

    private lateinit var subscriptionsManager: AggregateSubscriptionsManager// = AggregateSubscriptionsManager(eventStreamManager, registry, eventMapper)

    private lateinit var demoESService: EventSourcingService<String, ProjectAggregate, ProjectAggregateState>

    @BeforeEach
    fun init() {
        cleanDatabase()
        registry.register(ProjectAggregate::class, ProjectAggregateState::class) {
            registerStateTransition(ProjectCreatedEvent::class, ProjectAggregateState::projectCreatedApply)
            registerStateTransition(TagCreatedEvent::class, ProjectAggregateState::tagCreatedApply)
            registerStateTransition(TaskCreatedEvent::class, ProjectAggregateState::taskCreatedApply)
            registerStateTransition(TagAssignedToTaskEvent::class, ProjectAggregateState::tagAssignedApply)
        }

        eventStreamReaderManager = EventStoreStreamReaderManager(eventStore, properties)

        eventStreamManager = AggregateEventStreamManager(registry, eventStore, properties, eventStreamReaderManager)
        subscriptionsManager = AggregateSubscriptionsManager(eventStreamManager, registry, eventMapper)

        eventStreamManager.maintenance {
            onReadIndexCommitted { streamName, index ->
                println("Index committed, streamName: $index")
            }
        }

        demoESService = EventSourcingService(
            aggregateClass = ProjectAggregate::class,
            aggregateRegistry = registry,
            eventStore = eventStore,
            eventMapper = eventMapper,
            eventSourcingProperties = properties
        )
    }

    @Test
    fun checkActiveReaderIntercepted() {
        var eventsPublished = 0

        val start = System.currentTimeMillis()
        val durations = mutableListOf<Long>()

        (1..numberOfProjects).map { projectId ->
            demoESService.create {
                eventsPublished++
                it.create(projectId.toString())
            }
        }.forEach { createdEvent ->
            CoroutineScope(dispatcher).launch {
                for (i in 1..numberOfTagsPerProject) {
                    demoESService.update(createdEvent.projectId) {
                        eventsPublished++
                        it.createTag("Tag - $i")
                    }
                }
                durations.add(System.currentTimeMillis())
            }
        }

        val eventStreamManager1 =
            AggregateEventStreamManager(registry, eventStore, properties, eventStreamReaderManager)
        val eventStreamManager2 =
            AggregateEventStreamManager(registry, eventStore, properties, eventStreamReaderManager)

        val results = ArrayBlockingQueue<StreamHandleResult>(totalNumberOfEvents * 2) // 10500(всего eventов) * 2 стрима
        val eventCounter = AtomicInteger(0)
        val switchingCounter = AtomicInteger(0)

        val stream1 =
            eventStreamManager1.createEventStream("test-active-subscribers-stream", ProjectAggregate::class).also {
                CoroutineScope(dispatcher).launch {
                    while (true) {
                        it.handleNextRecord {
                            eventCounter.incrementAndGet()
                            results.add(StreamHandleResult(1, it.id))
                            true
                        }
                    }
                }
            }

        val stream2 =
            eventStreamManager2.createEventStream("test-active-subscribers-stream", ProjectAggregate::class).also {
                CoroutineScope(dispatcher).launch {
                    while (true) {
                        it.handleNextRecord {
                            eventCounter.incrementAndGet()
                            results.add(StreamHandleResult(2, it.id))
                            true
                        }
                    }
                }
            }

        val random = Random()
        CoroutineScope(dispatcher).launch {
            val compositeStream = CompositeEventStream(stream1, stream2)
            while (true) {
                if (random.nextDouble() > streamNotFailingProbability) {
                    compositeStream.switchActive()
                    switchingCounter.incrementAndGet()
                }
                delay(properties.maxActiveReaderInactivityPeriod.inWholeMilliseconds)
            }
        }

        runBlocking {
            await.atMost(200, TimeUnit.SECONDS).pollDelay(ofMillis(500)).until {
                println("NUM: ${results.distinctBy { it.eventId }.count()}, ${eventCounter.get()}")
                results.distinctBy { it.eventId }.count() == totalNumberOfEvents
            }
            // todo sukhoa: ofc test should be rewritten to have some reasonable asserts. Also it shows that we loose events :)
            println("First stream processed: ${results.count { it.streamId == 1 }}")
            println("Second stream processed: ${results.count { it.streamId == 2 }}")
            println(
                "Duplicates stream processed: ${
                    results.groupBy { it.eventId }.filter { it.value.size > 1 }.count()
                }"
            )
            println("There were: ${switchingCounter.get()} switches")
            println("ES updates take ${durations.maxOf { it } - start}ms")
        }
    }

    @Test
    fun hasActiveReader_ReturnsTrue() {
        val eventStoreStreamReaderManager = EventStoreStreamReaderManager(eventStore, properties)
        val reader = UUID.randomUUID().toString()

        eventStoreStreamReaderManager.tryUpdateReaderState("test-stream", reader, readingIndex = 0L)

        val hasActiveReader = eventStoreStreamReaderManager.hasActiveReader("test-stream")

        assertTrue(hasActiveReader)
    }

    @Test
    fun hasActiveReader_ReturnsFalse() {
        val eventStoreStreamReaderManager = EventStoreStreamReaderManager(eventStore, properties)
        val reader = UUID.randomUUID().toString()

        eventStoreStreamReaderManager.tryUpdateReaderState("test-stream", reader, readingIndex = 0L)

        val waitTimeMillis =
            properties.maxActiveReaderInactivityPeriod.inWholeMilliseconds + properties.eventReaderHealthCheckPeriod.inWholeMilliseconds

        await.atMost(waitTimeMillis, TimeUnit.MILLISECONDS).until {
            !eventStoreStreamReaderManager.hasActiveReader("test-stream")
        }
    }

    class CompositeEventStream(
        private val stream1: AggregateEventStream<ProjectAggregate>,
        private val stream2: AggregateEventStream<ProjectAggregate>
    ) {
        var active1 = true
        var active2 = true

        fun switchActive() {
            when {
                active1 && active2 -> {
                    stream1.suspend()
                    active1 = false
                    println("Switched from 1 to 2 initially")
                }
                !active1 && active2 -> {
                    stream1.resume()
                    active1 = true
                    stream2.suspend()
                    active2 = false
                    println("Switched from 2 to 1")
                }
                active1 && !active2 -> {
                    stream2.resume()
                    active2 = true
                    stream1.suspend()
                    active1 = false
                    println("Switched from 1 to 2")
                }
                else -> Unit // throw IllegalStateException("Both streams are down")
            }
        }
    }

    data class StreamHandleResult(
        val streamId: Int,
        val eventId: String,
    )
}