package ru.quipy.streams

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.quipy.domain.Aggregate
import ru.quipy.domain.EventRecord
import java.util.concurrent.atomic.AtomicBoolean


class BufferedAggregateEventStream<A : Aggregate>(
    override val streamName: String,
    private val streamReadPeriod: Long, // todo sukhoa wrong naming
    private val streamBatchSize: Int,
    private val eventsChannel: EventsChannel,
    private val eventReader: EventReader,
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
                    logger.info("Suspending stream $streamName...")
                    delay(5_000)
                }

                val eventsBatch = eventReader.read(streamBatchSize)

                if (eventsBatch.isEmpty()) {
                    delay(streamReadPeriod)
                    continue
                }
            }
        }.also {
            it.invokeOnCompletion(eventStreamCompletionHandler)
        }

    override suspend fun handleNextRecord(eventProcessingFunction: suspend (EventRecord) -> Boolean) {
        val receive = eventsChannel.receiveEvent()
        try {
            eventProcessingFunction(receive.record).also {
                if (!it) logger.info("Processing function return false for event record: ${receive.record} at index: ${receive.readIndex}`")
                eventsChannel.sendConfirmation(EventsChannel.EventConsumedAck(receive.readIndex, successful = it))
            }
        } catch (e: Exception) {
            logger.error(
                "Error while invoking event handling function at index: ${receive.readIndex} event record: ${receive.record}",
                e
            )

            eventsChannel.sendConfirmation(EventsChannel.EventConsumedAck(receive.readIndex, successful = false))
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
    }

    override fun resume() {
        logger.info("Resuming stream $streamName...")
        suspended.set(false)
    }
}