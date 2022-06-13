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
interface AggregateEventStream<A : Aggregate> {
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

interface EventStreamListener { // todo sukhoa better naming
    fun onStreamLaunched(block: (streamName: String) -> Unit)

    fun onReadIndexSynced(block: (streamName: String, index: Long) -> Unit)

    fun onStreamReset(block: (streamName: String, resetIndex: Long) -> Unit)

    fun onBatchRead(block: (streamName: String, batchSize: Int) -> Unit)

    fun onRecordHandledSuccessfully(block: (streamName: String) -> Unit)

    fun onRecordHandlingRetry(block: (streamName: String, eventName: String, retryAttempt: Int) -> Unit)

    fun onRecordSkipped(block: (streamName: String, eventName: String, retryAttempt: Int) -> Unit)

    fun onStreamSuspended(block: (streamName: String, reason: String, duration: Long) -> Unit)

    fun onReadIndexCommitted(block: (streamName: String, index: Long) -> Unit)
}

interface EventStreamNotifier { // todo sukhoa better naming
    fun onStreamLaunched(streamName: String)

    fun onReadIndexSynced(streamName: String, index: Long)

    fun onStreamReset(streamName: String, resetIndex: Long)

    fun onBatchRead(streamName: String, batchSize: Int)

    fun onRecordHandledSuccessfully(streamName: String)

    fun onRecordHandlingRetry(streamName: String, eventName: String, retryAttempt: Int)

    fun onRecordSkipped(streamName: String, eventName: String, retryAttempt: Int)

    fun onStreamSuspended(streamName: String, reason: String, duration: Long)

    fun onReadIndexCommitted(streamName: String, index: Long)
}

class EventStreamListenerImpl : EventStreamListener, EventStreamNotifier {
    private val onLaunchedHandlers: MutableList<(streamName: String) -> Unit> = mutableListOf()
    private val onIndexSyncedHandlers: MutableList<(streamName: String, index: Long) -> Unit> = mutableListOf()
    private val onStreamResetHandlers: MutableList<(streamName: String, resetIndex: Long) -> Unit> = mutableListOf()
    private val onBatchReadHandlers: MutableList<(streamName: String, batchSize: Int) -> Unit> = mutableListOf()
    private val onRecordHandledSuccessfullyHandlers: MutableList<(streamName: String) -> Unit> = mutableListOf()
    private val onRecordHandlingRetryHandlers: MutableList<(streamName: String, eventName: String, retryAttempt: Int) -> Unit> =
        mutableListOf()
    private val onRecordSkippedHandlers: MutableList<(streamName: String, eventName: String, retryAttempt: Int) -> Unit> =
        mutableListOf()
    private val onStreamSuspendedHandlers: MutableList<(streamName: String, reason: String, duration: Long) -> Unit> =
        mutableListOf()
    private val onReadIndexCommittedHandlers: MutableList<(streamName: String, index: Long) -> Unit> = mutableListOf()

    override fun onStreamLaunched(block: (streamName: String) -> Unit) {
        onLaunchedHandlers.add(block)
    }

    override fun onReadIndexSynced(block: (streamName: String, index: Long) -> Unit) {
        onIndexSyncedHandlers.add(block)
    }

    override fun onStreamReset(block: (streamName: String, resetIndex: Long) -> Unit) {
        onStreamResetHandlers.add(block)
    }

    override fun onBatchRead(block: (streamName: String, batchSize: Int) -> Unit) {
        onBatchReadHandlers.add(block)
    }

    override fun onRecordHandledSuccessfully(block: (streamName: String) -> Unit) {
        onRecordHandledSuccessfullyHandlers.add(block)
    }

    override fun onRecordHandlingRetry(block: (streamName: String, eventName: String, retryAttempt: Int) -> Unit) {
        onRecordHandlingRetryHandlers.add(block)
    }

    override fun onRecordSkipped(block: (streamName: String, eventName: String, retryAttempt: Int) -> Unit) {
        onRecordSkippedHandlers.add(block)
    }

    override fun onStreamSuspended(block: (streamName: String, reason: String, duration: Long) -> Unit) {
        onStreamSuspendedHandlers.add(block)
    }

    override fun onReadIndexCommitted(block: (streamName: String, index: Long) -> Unit) {
        onReadIndexCommittedHandlers.add(block)
    }

    override fun onStreamLaunched(streamName: String) {
        onLaunchedHandlers.forEach {
            it.invoke(streamName)
        }
    }

    override fun onReadIndexSynced(streamName: String, index: Long) {
        onIndexSyncedHandlers.forEach {
            it.invoke(streamName, index)
        }
    }

    override fun onStreamReset(streamName: String, resetIndex: Long) {
        onStreamResetHandlers.forEach {
            it.invoke(streamName, resetIndex)
        }
    }

    override fun onBatchRead(streamName: String, batchSize: Int) {
        onBatchReadHandlers.forEach {
            it.invoke(streamName, batchSize)
        }
    }

    override fun onRecordHandledSuccessfully(streamName: String) {
        onRecordHandledSuccessfullyHandlers.forEach {
            it.invoke(streamName)
        }
    }

    override fun onRecordHandlingRetry(streamName: String, eventName: String, retryAttempt: Int) {
        onRecordHandlingRetryHandlers.forEach {
            it.invoke(streamName, eventName, retryAttempt)
        }
    }

    override fun onRecordSkipped(streamName: String, eventName: String, retryAttempt: Int) {
        onRecordSkippedHandlers.forEach {
            it.invoke(streamName, eventName, retryAttempt)
        }
    }

    override fun onStreamSuspended(streamName: String, reason: String, duration: Long) {
        onStreamSuspendedHandlers.forEach {
            it.invoke(streamName, reason, duration)
        }
    }

    override fun onReadIndexCommitted(streamName: String, index: Long) {
        onReadIndexCommittedHandlers.forEach {
            it.invoke(streamName, index)
        }
    }
}


