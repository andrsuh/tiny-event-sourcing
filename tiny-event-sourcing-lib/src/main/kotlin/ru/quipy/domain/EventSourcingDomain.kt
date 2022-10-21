package ru.quipy.domain

import java.util.*

interface Versioned {
    var version: Long
}

interface Unique<ID> { // todo sukhoa rename this stuff
    val id: ID
}

interface Aggregate

interface AggregateState<ID, A : Aggregate> { // todo sukhoa add version of the state
    /**
     * Returns the ID of the aggregate or null if state is empty
     */
    fun getId(): ID?
}

fun interface AggregateStateTransitionFunction<A : Aggregate, E : Event<A>, S : AggregateState<*, A>> {
    fun performTransition(state: S, event: E)
}

abstract class Event<A : Aggregate>(
    override val id: UUID = UUID.randomUUID(),
    val name: String,
    override var version: Long = 0L, // this is aggregate version actually or the event count number
    var createdAt: Long = System.currentTimeMillis(),
) : Versioned, Unique<UUID>

@Suppress("unused")
data class EventRecord(
    override val id: String,
    val aggregateId: Any, // todo sukhoa weird?
    val aggregateVersion: Long,
    val eventTitle: String,
    val payload: String,
    val createdAt: Long = System.currentTimeMillis()
) : Unique<String>

@Suppress("UNCHECKED_CAST")
class Snapshot(
    override val id: Any, // todo sukhoa weird ANY, should it be parametrized?
    val snapshot: Any, // todo sukhoa weird ANY, should it be parametrized?
    override var version: Long
) : Versioned, Unique<Any>


class EventStreamReadIndex(
    override val id: String, // name of the stream
    val readIndex: Long,
    override var version: Long
) : Unique<String>, Versioned {
    override fun toString(): String {
        return "EventStreamReadIndex(id='$id', readIndex=$readIndex, version=$version)"
    }
}

class ActiveEventStreamReader(
    override val id: UUID,
    override var version: Long,
    val lastInteraction: Long
) : Unique<UUID>, Versioned