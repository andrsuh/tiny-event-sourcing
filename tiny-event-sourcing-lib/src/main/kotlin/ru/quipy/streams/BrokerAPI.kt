package ru.quipy.streams

import ru.quipy.domain.ExternalEventRecord
import ru.quipy.domain.Topic

interface Producer<T : Topic> {

    suspend fun sendEvents(partitionKey: String, externalEvents: List<ExternalEventRecord>)

    fun close()
}

interface Consumer<T : Topic> {

    fun startConsuming()

    fun poll(): List<ExternalEventRecord>

    fun close()
}

interface StoppableAndDestructible {

    fun stopAndDestroy()
}
