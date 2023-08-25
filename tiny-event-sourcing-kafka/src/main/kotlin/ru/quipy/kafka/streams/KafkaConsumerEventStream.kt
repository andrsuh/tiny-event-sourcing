package ru.quipy.kafka.streams

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.quipy.domain.ExternalEventRecord
import ru.quipy.domain.Topic
import ru.quipy.streams.EventStreamNotifier
import ru.quipy.streams.ExternalEventStream
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.concurrent.atomic.AtomicBoolean

class KafkaConsumerEventStream<T : Topic>(
    override val streamName: String,
    private val emptyEventBatchDelay: Long,
    private val eventsChannel: ExternalEventsChannel,
    private val kafkaEventConsumer: KafkaEventConsumer<T>,
    private val retryConfig: RetryConf,
    private val eventStreamNotifier: EventStreamNotifier,
    private val dispatcher: CoroutineDispatcher
) : ExternalEventStream<T> {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaConsumerEventStream::class.java)
    }

    private var active = AtomicBoolean(true)
    private var suspended = AtomicBoolean(false)

    private val eventStreamCompletionHandler: CompletionHandler = { th: Throwable? ->
        if (active.get()) {
            logger.error(
                "Unexpected error in external event stream ${streamName}. Relaunching...",
                th
            )
            eventStreamJob = launchJob()
        } else {
            logger.warn("Stopped external event stream $streamName coroutine")
        }
    }

    @Volatile
    private var eventStreamJob: Job = launchJob()

    private fun launchJob() =
        CoroutineScope(CoroutineName("reading-$streamName-coroutine") + dispatcher).launch {
            delay(5_000)
            eventStreamNotifier.onStreamLaunched(streamName)
            kafkaEventConsumer.startConsuming()

            while (active.get()) {
                while (suspended.get()) {
                    logger.debug("Suspending external event stream $streamName...")
                    delay(500)
                }

                val eventsBatch = kafkaEventConsumer.poll()

                if (eventsBatch.isEmpty()) {
                    delay(emptyEventBatchDelay)
                    continue
                }

                eventsBatch.forEach { eventRecord ->
                    logger.trace("Processing external event from batch: $eventRecord.")

                    feedToHandling(eventRecord) {
                        eventStreamNotifier.onRecordHandledSuccessfully(streamName, eventRecord.eventTitle)
                    }
                }
            }
        }.also {
            it.invokeOnCompletion(eventStreamCompletionHandler)
        }

    override suspend fun handleNextRecord(eventProcessingFunction: suspend (ExternalEventRecord) -> Boolean) {
        val receivedRecord = eventsChannel.receiveEvent()
        logger.trace("Event $receivedRecord was received for handling")

        try {
            eventProcessingFunction(receivedRecord).also {
                if (!it) logger.info("Processing function return false for event record: $receivedRecord")

                logger.trace("Sending confirmation on receiving event $receivedRecord")
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

        if (eventStreamJob.isActive) {
            eventStreamJob.cancel()
        }
    }

    override fun suspend() {
        suspended.set(true)
    }

    override fun resume() {
        logger.info("Resuming stream $streamName...")
        suspended.set(false)
    }

    override fun isSuspended() = suspended.get()

    private suspend fun feedToHandling(event: ExternalEventRecord, beforeNextPerform: () -> Unit) {
        for (attemptNum in 1..retryConfig.maxAttempts) {
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