package ru.quipy.streams

import ru.quipy.domain.Aggregate
import ru.quipy.domain.EventRecord

/**
 * Reads event records for the given type of aggregate in order they were written to DB and
 * allows to handle them one by one in same order.
 *
 * Regardless of implementation the event stream would not be overwhelmed by events as
 * it will be suspended in case nobody is consuming it's events. So it might be called "Lazy"
 */
interface AggregateEventsStream<A : Aggregate> {
    val streamName: String

    /**
     * Allows to handle next event. Suspends until event is supplied.
     */
    suspend fun handleNextRecord(eventProcessingFunction: suspend (EventRecord) -> Boolean)

    /**
     * We can "replay" events in the stream by resetting it to desired reading index
     */
    fun resetToReadingIndex(version: Long)

    /**
     * Stops process that reads events from DB
     */
    fun stopAndDestroy()
}

