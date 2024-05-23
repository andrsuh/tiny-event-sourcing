package ru.quipy.streams

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.quipy.domain.Aggregate
import ru.quipy.domain.EventRecord
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.concurrent.atomic.AtomicBoolean


class BufferedAggregateEventStream<A : Aggregate>(
        override val streamName: String,
        private val streamReadPeriod: Long, // todo sukhoa wrong naming
        private val streamBatchSize: Int,
        private val eventsChannel: EventsChannel,
        private val eventReader: EventReader,
        private val retryConfig: RetryConf,
        private val eventStreamNotifier: EventStreamNotifier,
        private val dispatcher: CoroutineDispatcher
) : AggregateEventStream<A> {
    companion object {
        private val logger = LoggerFactory.getLogger(BufferedAggregateEventStream::class.java)
    }

    private var active = AtomicBoolean(true)
    private var suspended = AtomicBoolean(false)

    private val eventStreamCompletionHandler: CompletionHandler = { th: Throwable? ->
        if (active.get()) {
            logger.error(
                    "Unexpected error in aggregate event stream ${streamName}. Relaunching...",
                    th
            )

            eventStreamJob = launchJob()
        } else {
            logger.warn("Stopped event stream $streamName coroutine")
        }
    }

    @Volatile
    private var eventStreamJob: Job = launchJob()

    private fun launchJob() =
            CoroutineScope(CoroutineName("reading-$streamName-coroutine") + dispatcher).launch {
                // initial delay
                delay(5_000)
                eventStreamNotifier.onStreamLaunched(streamName)

                while (active.get()) {
                    while (suspended.get()) {
                        logger.debug("Suspending stream $streamName...")
                        delay(500)
                    }

                    val eventsBatch = eventReader.read(streamBatchSize)

                    if (eventsBatch.isEmpty()) {
                        delay(streamReadPeriod)
                        continue
                    }

                    eventsBatch.forEach { eventRecord ->
                        logger.trace("Processing event from batch: {}.", eventRecord)

                        feedToHandling(eventRecord) {
                            eventStreamNotifier.onRecordHandledSuccessfully(streamName, eventRecord.eventTitle)
                            eventReader.acknowledgeRecord(eventRecord)
                        }
                    }
                }
            }.also {
                it.invokeOnCompletion(eventStreamCompletionHandler)
            }

    override suspend fun handleNextRecord(eventProcessingFunction: suspend (EventRecord) -> Boolean) {
        val receivedRecord = eventsChannel.receiveEvent()
        logger.trace("Event {} was received for handling", receivedRecord)

        try {
            eventProcessingFunction(receivedRecord).also {
                if (!it) logger.info("Processing function return false for event record: $receivedRecord")

                logger.trace("Sending confirmation on receiving event {}", receivedRecord)
                eventsChannel.sendConfirmation(isConfirmed = it)
            }
        } catch (e: Exception) {
            logger.error(
                    "Error while invoking event handling function. Stream: ${streamName}. Event record: $receivedRecord",
                    e
            )

            eventsChannel.sendConfirmation(isConfirmed = false)
        }
    }

    override fun stopAndDestroy() {
        if (!active.compareAndSet(true, false)) return
        // todo sukhoa think of committing last read index

        eventReader.stop()

        if (eventStreamJob.isActive) {
            eventStreamJob.cancel()
        }
    }

    override fun suspend() {
        suspended.set(true)
        eventReader.stop()
    }

    override fun resume() {
        logger.info("Resuming stream $streamName...")
        suspended.set(false)
        eventReader.resume()
    }

    override fun isSuspended() = suspended.get()

    private suspend fun feedToHandling(event: EventRecord, beforeNextPerform: suspend () -> Unit) {
        for (attemptNum in 1..retryConfig.maxAttempts) { // todo sukhoa BUG
            eventsChannel.sendEvent(event)

            if (eventsChannel.receiveConfirmation()) {
                beforeNextPerform()
                return
            }

            if (attemptNum == retryConfig.maxAttempts) {
                when (retryConfig.lastAttemptFailedStrategy) {
                    RetryFailedStrategy.SKIP_EVENT -> {
                        logger.error("Event stream: $streamName. Retry attempts failed $attemptNum times. SKIPPING...")
                        beforeNextPerform()
                        return
                    }
                    RetryFailedStrategy.SUSPEND -> {
                        logger.error("Event stream: $streamName. Retry attempts failed $attemptNum times. SUSPENDING THE HOLE STREAM...")
                        eventStreamNotifier.onRecordSkipped(streamName, event.eventTitle, attemptNum)
                        delay(Long.MAX_VALUE) // todo sukhoa find the way better
                    }
                }
            }
            eventStreamNotifier.onRecordHandlingRetry(streamName, event.eventTitle, attemptNum)
        }
    }
}