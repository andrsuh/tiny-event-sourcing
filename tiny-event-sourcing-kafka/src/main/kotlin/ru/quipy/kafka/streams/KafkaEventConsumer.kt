package ru.quipy.kafka.streams

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.KafkaException
import org.slf4j.LoggerFactory
import ru.quipy.domain.ExternalEventRecord
import ru.quipy.domain.Topic
import ru.quipy.kafka.core.KafkaProperties
import ru.quipy.streams.Consumer
import java.time.Duration
import java.util.*

/**
 * [KafkaEventConsumer] is a Kafka implementation of [Consumer] interface.
 *
 * It is designed to consume [ExternalEventRecord]s from a Kafka topic. It uses a [KafkaConsumer] internally.
 */
class KafkaEventConsumer<T : Topic>(
    private val topicName: String,
    private val groupId: String,
    private val kafkaProperties: KafkaProperties,
    private val objectMapper: ObjectMapper
) : Consumer<T> {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaEventConsumer::class.java)
    }

    private val consumer: KafkaConsumer<String, String> = createConsumer()

    override fun startConsuming() {
        consumer.subscribe(listOf(topicName))
        logger.info("Started consuming from $topicName topic")
    }

    override fun poll(): List<ExternalEventRecord> {
        try {
            val records = consumer.poll(Duration.ofMillis(kafkaProperties.pollingTimeoutMs))

            val events = records.mapNotNull { record ->
                runCatching {
                    objectMapper.readValue(record.value(), ExternalEventRecord::class.java)
                }.onFailure { e ->
                    logger.error("Failed to deserialize event: ${record.value()}", e)
                }.getOrNull()
            }
            return events
        } catch (e: KafkaException) {
            logger.error("Failed to poll events from $topicName topic", e)
            throw e
        } catch (e: Exception) {
            logger.error("An unexpected exception occurred: ${e.message}", e)
            throw e
        }
    }

    override fun close() {
        consumer.close()
    }

    fun commitOffset() {
        consumer.commitSync()
        logger.info("Committed offset for $topicName topic")
    }

    private fun createConsumer(): KafkaConsumer<String, String> {
        val props = Properties()

        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = "group-id-config-$groupId"
        props[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_committed"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"

        return KafkaConsumer<String, String>(props)
    }
}
