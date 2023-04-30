package ru.quipy.streams

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingProperties
import ru.quipy.database.EventStore
import ru.quipy.domain.ActiveEventStreamReader

interface EventStreamReaderManager {
    fun findActiveReader(streamName: String): ActiveEventStreamReader?
    fun hasActiveReader(streamName: String): Boolean
    fun tryInterceptReading(streamName: String, readerId: String): Boolean
    fun tryUpdateReaderState(streamName: String, readerId: String, readingIndex: Long): Boolean
}

class ActiveEventStreamReaderManager(
    private val eventStore: EventStore,
    private val config: EventSourcingProperties
) : EventStreamReaderManager {
    private val logger: Logger = LoggerFactory.getLogger(ActiveEventStreamReaderManager::class.java)

    override fun findActiveReader(streamName: String): ActiveEventStreamReader? {
        return eventStore.getActiveStreamReader(streamName)
    }

    override fun hasActiveReader(streamName: String): Boolean {
        val activeStreamReader: ActiveEventStreamReader = eventStore.getActiveStreamReader(streamName) ?: return false
        val lastInteraction = activeStreamReader.lastInteraction
        val currentTime = System.currentTimeMillis()

        if (currentTime - lastInteraction > config.maxActiveReaderInactivityPeriod.inWholeMilliseconds) {
            logger.warn("Reader of stream $streamName is not alive. Last interaction time: $lastInteraction.")
            return false
        }

        logger.debug("Reader of stream $streamName is alive. Last interaction time: $lastInteraction.")
        return true
    }

    override fun tryInterceptReading(streamName: String, readerId: String): Boolean {
        val currentActiveReader: ActiveEventStreamReader? = eventStore.getActiveStreamReader(streamName)

        if (currentActiveReader != null && currentActiveReader.readerId == readerId) {
            logger.info("An attempt to intercept reading of stream $streamName by its active reader $readerId")
            return false
        }

        val newActiveReader = createNewActiveReader(streamName, readerId, currentActiveReader)

        val expectedVersion = currentActiveReader?.version ?: 0

        if (eventStore.tryReplaceActiveStreamReader(expectedVersion, newActiveReader)) {
            logger.info("Event stream reader of stream $streamName has been switched from [${currentActiveReader?.readerId}] to [${newActiveReader.readerId}]")
            return true
        }

        return false
    }

    override fun tryUpdateReaderState(streamName: String, readerId: String, readingIndex: Long): Boolean {
        val activeReader: ActiveEventStreamReader? = eventStore.getActiveStreamReader(streamName)

        val version = if (activeReader?.version != null) activeReader.version + 1 else 1

        val updatedActiveReader = ActiveEventStreamReader(
                activeReader?.id ?: streamName,
                version,
                readerId,
                readingIndex,
                lastInteraction = System.currentTimeMillis(),
        )

        return eventStore.tryUpdateActiveStreamReader(updatedActiveReader)
    }

    private fun createNewActiveReader(streamName: String, readerId: String, currentActiveReader: ActiveEventStreamReader?): ActiveEventStreamReader {
        val newVersion: Long = (currentActiveReader?.version ?: 1) + 1
        val readPosition: Long = currentActiveReader?.readPosition ?: 1
        val lastInteraction: Long = System.currentTimeMillis()

        return ActiveEventStreamReader(streamName, newVersion, readerId, readPosition, lastInteraction)
    }
}