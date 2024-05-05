package ru.quipy

import org.awaitility.kotlin.await
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.quipy.core.EventSourcingProperties
import ru.quipy.projectDemo.api.ProjectAggregate
import ru.quipy.projectDemo.api.TagCreatedEvent
import ru.quipy.projectDemo.create
import ru.quipy.projectDemo.createTag
import java.util.concurrent.TimeUnit

class StreamEventOrderingTest: BaseTest(testId) {
    companion object {
        const val testId = "StreamEventOrderingTest"
        @BeforeAll
        @JvmStatic
        fun configure() {
            configure(eventSourcingProperties = EventSourcingProperties(streamBatchSize = 3))
        }
    }

    private val sb = StringBuilder()

    @BeforeEach
    fun init() {
        cleanDatabase()
    }

    @Test
    fun testEventOrder() {
        demoESService.create {
            it.create(testId)
        }

        demoESService.update(testId) {
            it.createTag("1")
        }
        demoESService.update(testId) {
            it.createTag("2")
        }
        demoESService.update(testId) {
            it.createTag("3")
        }
        demoESService.update(testId) {
            it.createTag("4")
        }
        demoESService.update(testId) {
            it.createTag("5")
        }
        demoESService.update(testId) {
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