package ru.quipy.streams

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

interface EventStreamReaderManager {
    fun isReaderAlive(streamName: String): Boolean
    fun tryInterceptReading(streamName: String): Boolean
    fun updateReaderState(streamName: String, readingIndex: Long)
}

class ActiveEventStreamReaderManager(
    private val eventStore: EventStore,
) : EventStreamReaderManager {
    private val logger: Logger = LoggerFactory.getLogger(ActiveEventStreamReaderManager::class.java)
    private val maxActiveReaderInactivityPeriod: Duration = 5.minutes

    override fun isReaderAlive(streamName: String): Boolean {
        val activeStreamReader: ActiveEventStreamReader = eventStore.getActiveStreamReader(streamName) ?: return false
        val lastInteraction = activeStreamReader.lastInteraction
        val currentTime = System.currentTimeMillis()

        if (currentTime - lastInteraction > maxActiveReaderInactivityPeriod.inWholeMilliseconds) {
            logger.warn("Reader of stream $streamName is not alive. Last interaction time: $lastInteraction.")
            return false
        }

        logger.debug("Reader of stream $streamName is alive. Last interaction time: $lastInteraction.")
        return true
    }

    override fun tryInterceptReading(streamName: String): Boolean {
        val currentActiveReader: ActiveEventStreamReader? = eventStore.getActiveStreamReader(streamName)
        val newActiveReader = createNewActiveReader(streamName, currentActiveReader)

        val expectedVersion = currentActiveReader?.version ?: 0

        if (eventStore.tryReplaceActiveStreamReader(expectedVersion, newActiveReader)) {
            logger.info("Event stream reader of stream $streamName has been switched from [${currentActiveReader?.id}] to [${newActiveReader.id}]")
            return true
        }

        return false
    }

    override fun updateReaderState(streamName: String, readingIndex: Long) {
        val activeReader: ActiveEventStreamReader? = eventStore.getActiveStreamReader(streamName)

        val updatedActiveReader = ActiveEventStreamReader(
            activeReader?.id ?: streamName,
            activeReader?.version ?: 1,
            readingIndex,
            lastInteraction = System.currentTimeMillis(),
        )

        eventStore.updateActiveStreamReader(updatedActiveReader)
    }

    private fun createNewActiveReader(streamName: String, currentActiveReader: ActiveEventStreamReader?): ActiveEventStreamReader {
        val newVersion: Long = (currentActiveReader?.version ?: 1) + 1
        val readPosition: Long = currentActiveReader?.readPosition ?: 1
        val lastInteraction: Long = System.currentTimeMillis()

        return ActiveEventStreamReader(streamName, newVersion, readPosition, lastInteraction)
    }
}