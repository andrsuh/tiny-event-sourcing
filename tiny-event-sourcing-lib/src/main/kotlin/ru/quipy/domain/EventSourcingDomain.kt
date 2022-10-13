package ru.quipy.domain

import java.util.*

interface Versioned {
    var version: Long
}

interface Unique<ID> { // todo sukhoa rename this stuff
    val id: ID
}

interface Aggregate

interface AggregateState<ID, A : Aggregate> {
    val aggregateId: ID
    var createdAt: Long
    var updatedAt: Long
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
class Snapshot<T, E>(
    override val id : T, // todo sukhoa weird ANY, should it be parametrized?
    val snapshot : E, // todo sukhoa weird ANY, should it be parametrized?
    override var version: Long
) : Versioned, Unique<T>


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