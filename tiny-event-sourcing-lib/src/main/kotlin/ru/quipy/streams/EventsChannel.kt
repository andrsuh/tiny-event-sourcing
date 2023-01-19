package ru.quipy.streams

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import ru.quipy.domain.EventRecord

class EventsChannel {
    private val eventsChannel: Channel<EventRecordForHandling> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    private val acknowledgesChannel: Channel<EventConsumedAck> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    suspend fun sendEvent(eventRecord: EventRecordForHandling) {
        eventsChannel.send(eventRecord)
    }

    suspend fun receiveEvent(): EventRecordForHandling {
        return eventsChannel.receive()
    }

    suspend fun sendConfirmation(confirmation: EventConsumedAck) {
        acknowledgesChannel.send(confirmation)
    }

    suspend fun receiveConfirmation(): Boolean {
        val eventConsumedAck = acknowledgesChannel.receive()
        return eventConsumedAck.successful
    }

    class EventRecordForHandling(
        val readIndex: Long,
        val record: EventRecord,
    )

    class EventConsumedAck(
        val readIndex: Long,
        val successful: Boolean,
    )
}