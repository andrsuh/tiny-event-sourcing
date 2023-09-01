package ru.quipy.streams

import ru.quipy.domain.ExternalEventRecord
import ru.quipy.domain.Topic

/**
 * [Producer] representing a producer for sending external events to a specific topic.
 *
 * Implementations of this interface are responsible for sending batches of external events to a topic
 * identified by the provided partition key. The events to be sent are specified as a list of
 * [ExternalEventRecord] objects.
 */
interface Producer<T : Topic> {

    suspend fun sendEvents(partitionKey: String, externalEvents: List<ExternalEventRecord>)

    fun close()
}

/**
 * [Consumer] representing a consumer for consuming external events from a specific topic.
 *
 * Implementations of this interface are responsible for starting the consumption of external events
 * from a topic, polling for events, and closing the consumer when necessary.
 */
interface Consumer<T : Topic> {

    fun startConsuming()

    fun poll(): List<ExternalEventRecord>

    fun close()
}

/**
 * [StoppableAndDestructible] representing an entity (Subscribers) that can be stopped and destroyed.
 *
 * Implementations of this interface are responsible for providing a mechanism to stop and destroy
 * the entity, typically used to clean up resources or gracefully terminate its operation.
 */
interface StoppableAndDestructible {

    fun stopAndDestroy()
}
