package ru.quipy.updateSerial

import ru.quipy.core.annotations.AggregateType
import ru.quipy.core.annotations.DomainEvent
import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.Aggregate
import ru.quipy.domain.AggregateState
import ru.quipy.domain.Event
import ru.quipy.updateSerial.UpdateSerialTest.Companion.CREATED_EVENT_NAME
import ru.quipy.updateSerial.UpdateSerialTest.Companion.TEST_EVENT_NAME_1
import ru.quipy.updateSerial.UpdateSerialTest.Companion.TEST_EVENT_NAME_2
import ru.quipy.updateSerial.UpdateSerialTest.Companion.TEST_TABLE_NAME
import java.util.*

@AggregateType(aggregateEventsTableName = TEST_TABLE_NAME)
class TestAggregate : Aggregate

class TestAggregateState : AggregateState<UUID, TestAggregate> {
    private lateinit var id: UUID
    private var order: Int = 0

    override fun getId() = id

    fun create(id: UUID) = TestCreatedEvent(id)

    @StateTransitionFunc
    fun create(event: TestCreatedEvent) {
        id = event.testId
    }

    /**
     * This command generates the list of different type of events like it can be in a real world
     */
    fun testUpdateSerial(size: Int) = List(size) {index ->
            when (index % 2) {
                0 -> TestEvent_1(order + index + 1)
                else -> TestEvent_2(order + index + 1)
            }
    }

    @StateTransitionFunc
    fun testUpdateSerial(event: TestEvent_1) {
        order = event.order
    }

    @StateTransitionFunc
    fun testUpdateSerial(event: TestEvent_2) {
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

@DomainEvent(name = TEST_EVENT_NAME_1)
class TestEvent_1(
    val order: Int
) : Event<TestAggregate>(
    name = TEST_EVENT_NAME_1,
    createdAt = System.currentTimeMillis(),
)

@DomainEvent(name = TEST_EVENT_NAME_2)
class TestEvent_2(
    val order: Int
) : Event<TestAggregate>(
    name = TEST_EVENT_NAME_2,
    createdAt = System.currentTimeMillis(),
)