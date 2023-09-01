package ru.quipy.streams

import ru.quipy.domain.ExternalEventRecord
import ru.quipy.domain.EventRecord
import ru.quipy.domain.Topic

/**
 * Represents an external event stream for a specific topic, allowing the sequential handling of external event records.
 *
 * This interface defines methods for interacting with an external event stream, similar to the [AggregateEventStream].
 * It is designed for reading [EventRecord]s for a particular topic from an external source, such as a message queue,
 * and processing them one by one in the order they were received.
 */
interface ExternalEventStream<T : Topic> {

    val streamName: String

    suspend fun handleNextRecord(eventProcessingFunction: suspend (ExternalEventRecord) -> Boolean)

    fun stopAndDestroy()

    fun suspend()

    fun resume()

    fun isSuspended(): Boolean
}
