package ru.quipy.streams

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import ru.quipy.domain.EventRecord

class EventsChannel {
    private val eventsChannel: Channel<EventRecord> = Channel(
            capacity = Channel.RENDEZVOUS,
            onBufferOverflow = BufferOverflow.SUSPEND
    )

    private val acknowledgesChannel: Channel<Boolean> = Channel(
            capacity = Channel.RENDEZVOUS,
            onBufferOverflow = BufferOverflow.SUSPEND
    )

    suspend fun sendEvent(eventRecord: EventRecord) {
        eventsChannel.send(eventRecord)
    }

    suspend fun receiveEvent(): EventRecord {
        return eventsChannel.receive()
    }

    suspend fun sendConfirmation(isConfirmed: Boolean) {
        acknowledgesChannel.send(isConfirmed)
    }

    suspend fun receiveConfirmation(): Boolean {
        return acknowledgesChannel.receive()
    }
}