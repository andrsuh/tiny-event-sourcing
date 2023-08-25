package ru.quipy.streams

import ru.quipy.domain.ExternalEventRecord

interface Producer {

    suspend fun sendEvents(partitionKey: String, externalEvents: List<ExternalEventRecord>)

    fun close()
}

interface Consumer {

    fun startConsuming()

    fun poll(): List<ExternalEventRecord>

    fun close()
}
