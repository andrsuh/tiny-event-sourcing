package ru.quipy.domain

import java.util.*

interface Versioned {
    var version: Long
}

interface Unique<ID> { // todo sukhoa rename this stuff
    val id: ID
}

// todo sukhoa maybe AggregateState?
interface Aggregate { // todo sukhoa ID should be parametrized Unique<ID>
    val aggregateId: String
    var createdAt: Long
    var updatedAt: Long
}

abstract class Event<A : Aggregate>(
    override val id: UUID = UUID.randomUUID(),
    val name: String,
    var aggregateId: String, // todo sukhoa maybe val?
    override var version: Long = 0L, // this is aggregate version actually or the event count number
    var createdAt: Long = System.currentTimeMillis(),
) : Versioned, Unique<UUID> {
    abstract infix fun applyTo(aggregate: A)
}

@Suppress("unused")
data class EventRecord(
    override val id: String,
    val aggregateId: String,
    val aggregateVersion: Long,
    val eventTitle: String,
    val payload: String,
    val createdAt: Long = System.currentTimeMillis()
) : Unique<String>

@Suppress("UNCHECKED_CAST")
class Snapshot(
    override val id: String,
    val snapshot: Any, // todo sukhoa test how it works with adding properties to snapshot.
    override var version: Long
) : Versioned, Unique<String>


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