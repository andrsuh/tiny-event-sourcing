package ru.quipy.kafka.streams

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.ExternalEventRecord

/**
 * [ExternalEventsChannel] is a channel that is used to send and receive [ExternalEvent]'s.
 */
class ExternalEventsChannel {
    private val eventsChannel: Channel<ExternalEventRecord> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    private val acknowledgesChannel: Channel<Boolean> = Channel(
        capacity = Channel.RENDEZVOUS,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    suspend fun sendEvent(eventRecord: ExternalEventRecord) {
        eventsChannel.send(eventRecord)
    }

    suspend fun receiveEvent(): ExternalEventRecord {
        return eventsChannel.receive()
    }

    suspend fun sendConfirmation(isConfirmed: Boolean) {
        acknowledgesChannel.send(isConfirmed)
    }

    suspend fun receiveConfirmation(): Boolean {
        return acknowledgesChannel.receive()
    }

}