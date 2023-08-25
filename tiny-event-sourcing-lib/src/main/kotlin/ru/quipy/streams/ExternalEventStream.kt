package ru.quipy.streams

import ru.quipy.domain.ExternalEventRecord
import ru.quipy.domain.Topic

interface ExternalEventStream<T : Topic> {

    val streamName: String

    suspend fun handleNextRecord(eventProcessingFunction: suspend (ExternalEventRecord) -> Boolean)

    fun stopAndDestroy()

    fun suspend()

    fun resume()

    fun isSuspended(): Boolean
}
