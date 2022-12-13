package ru.quipy.updateSerial

import ru.quipy.core.annotations.AggregateType
import ru.quipy.core.annotations.DomainEvent
import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.Aggregate
import ru.quipy.domain.AggregateState
import ru.quipy.domain.Event
import ru.quipy.updateSerial.UpdateSerialTest.Companion.CREATED_EVENT_NAME
import ru.quipy.updateSerial.UpdateSerialTest.Companion.TEST_EVENT_NAME
import ru.quipy.updateSerial.UpdateSerialTest.Companion.TEST_TABLE_NAME
import java.util.*

@AggregateType(aggregateEventsTableName = TEST_TABLE_NAME)
class TestAggregate: Aggregate

class TestAggregateState : AggregateState<UUID, TestAggregate> {
    private lateinit var id : UUID
    private var order: Int = 0

    override fun getId() = id

    fun create(id: UUID) = TestCreatedEvent(id)

    @StateTransitionFunc
    fun create(event: TestCreatedEvent) {
        id = event.testId
    }

    fun testUpdateSerial(size: Int) = List(size) { TestUpdateSerialEvent(it) }

    @StateTransitionFunc
    fun testUpdateSerial(event: TestUpdateSerialEvent) {
        order = event.order
    }
}

@DomainEvent(name = CREATED_EVENT_NAME)
class TestCreatedEvent(
    val testId: UUID,
) : Event<TestAggregate>(
    name = CREATED_EVENT_NAME,
    createdAt = System.currentTimeMillis(),
)

@DomainEvent(name = TEST_EVENT_NAME)
class TestUpdateSerialEvent(
    val order : Int
) : Event<TestAggregate>(
    name = TEST_EVENT_NAME,
    createdAt = System.currentTimeMillis(),
)