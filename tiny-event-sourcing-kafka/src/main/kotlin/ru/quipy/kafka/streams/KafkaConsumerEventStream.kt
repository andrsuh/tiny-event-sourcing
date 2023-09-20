package ru.quipy.kafka.streams

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.quipy.domain.ExternalEventRecord
import ru.quipy.domain.Topic
import ru.quipy.streams.EventStreamNotifier
import ru.quipy.streams.ExternalEventStream
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy

/**
 * Represents a Kafka-based event stream for a specific topic, allowing the sequential handling of external event records
 * received from a Kafka consumer.
 *
 * This class implements the [ExternalEventStream] interface and is designed to consume external event records from a Kafka topic.
 * It provides methods for handling Kafka events in a controlled and sequential manner.
 */
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

    @Volatile
    private var active = true

    @Volatile
    private var suspended = false

    private val eventStreamCompletionHandler: CompletionHandler = { th: Throwable? ->
        if (active) {
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

            while (active) {
                while (suspended) {
                    logger.debug("Suspending external event stream $streamName...")
                    delay(500)
                }

                val eventsBatch = kafkaEventConsumer.poll()

                eventStreamNotifier.onBatchRead(streamName, eventsBatch.size)

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
        if (!active) return
        active = false

        if (eventStreamJob.isActive) {
            eventStreamJob.cancel()
        }

        kafkaEventConsumer.close()
    }

    override fun suspend() {
        suspended = true
    }

    override fun resume() {
        logger.info("Resuming stream $streamName...")
        suspended = false
    }

    override fun isSuspended() = suspended

    private suspend fun feedToHandling(event: ExternalEventRecord, beforeNextPerform: () -> Unit) {
        for (attemptNum in 1..retryConfig.maxAttempts) {
            eventsChannel.sendEvent(event)

            if (eventsChannel.receiveConfirmation()) {
                beforeNextPerform()
                return
            }
            eventStreamNotifier.onRecordHandlingRetry(streamName, event.eventTitle, attemptNum)
        }

        when (retryConfig.lastAttemptFailedStrategy) {
            RetryFailedStrategy.SKIP_EVENT -> {
                logger.error("Event stream: $streamName. Retry attempts failed ${retryConfig.maxAttempts} times. SKIPPING...")
                beforeNextPerform()
            }

            RetryFailedStrategy.SUSPEND -> {
                if (suspended) {
                    logger.debug("Stream $streamName is already suspended.")
                } else {
                    logger.error("Event stream: $streamName. Retry attempts failed ${retryConfig.maxAttempts} times. SUSPENDING THE WHOLE STREAM...")
                    eventStreamNotifier.onRecordSkipped(streamName, event.eventTitle, retryConfig.maxAttempts)
                    delay(Long.MAX_VALUE)
                }
            }
        }
    }
}