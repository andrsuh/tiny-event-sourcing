package ru.quipy.streams

import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event

/**
 * Reads events of the given type of aggregate in order they were written to DB and
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
    suspend fun handleEvent(eventProcessingFunction: suspend (Event<A>) -> Boolean)

    /**
     * We can "replay" events in the stream by resetting it to desired aggregate version
     */
    fun resetToAggregateVersion(version: Long)

    /**
     * Stops process that reads events from DB
     */
    fun stopAndDestroy()
}

