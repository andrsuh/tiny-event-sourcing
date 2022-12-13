package ru.quipy

import org.awaitility.kotlin.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import ru.quipy.core.EventSourcingService
import ru.quipy.projectDemo.api.ProjectAggregate
import ru.quipy.projectDemo.api.TagCreatedEvent
import ru.quipy.projectDemo.create
import ru.quipy.projectDemo.createTag
import ru.quipy.projectDemo.logic.ProjectAggregateState
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.concurrent.TimeUnit

@SpringBootTest(properties = ["event.sourcing.stream-batch-size=3"])
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class StreamEventOrderingTest {
    companion object {
        const val testId = "3"
    }

    @Autowired
    private lateinit var esService: EventSourcingService<String, ProjectAggregate, ProjectAggregateState>

    @Autowired
    private lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    lateinit var mongoTemplate: MongoTemplate

    fun cleanDatabase() {
        mongoTemplate.remove(Query.query(Criteria.where("aggregateId").`is`(testId)), "aggregate-project")
        mongoTemplate.remove(Query.query(Criteria.where("_id").`is`(testId)), "snapshots")
    }

    private val sb = StringBuilder()

    @BeforeEach
    fun init() {
        cleanDatabase()
    }

    @Test
    fun testEventOrder() {
        esService.create {
            it.create(testId)
        }

        esService.update(testId) {
            it.createTag("1")
        }
        esService.update(testId) {
            it.createTag("2")
        }
        esService.update(testId) {
            it.createTag("3")
        }
        esService.update(testId) {
            it.createTag("4")
        }
        esService.update(testId) {
            it.createTag("5")
        }
        esService.update(testId) {
            it.createTag("6")
        }

        subscriptionsManager.createSubscriber(ProjectAggregate::class, "StreamEventOrderingTest") {
            `when`(TagCreatedEvent::class) { event ->
                sb.append(event.tagName).also {
                    println(sb.toString())
                }
            }
        }

        await.atMost(10, TimeUnit.MINUTES).until {
            sb.toString() == "123456"
        }
    }
}