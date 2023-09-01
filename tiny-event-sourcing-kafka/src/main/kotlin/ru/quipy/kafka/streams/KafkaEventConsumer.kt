package ru.quipy.kafka.streams

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
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
    private val kafkaProperties: KafkaProperties
) : Consumer<T> {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaEventConsumer::class.java)
        private val objectMapper = ObjectMapper()
    }

    private val consumer: KafkaConsumer<String, String> = createConsumer()

    override fun startConsuming() {
        consumer.subscribe(listOf(topicName))
    }

    override fun poll(): List<ExternalEventRecord> {
        val records = consumer.poll(Duration.ofSeconds(1))
        val events = mutableListOf<ExternalEventRecord>()

        for (record in records) {
            val eventJson = record.value()
            try {
                val event = objectMapper.readValue(eventJson, ExternalEventRecord::class.java) as ExternalEventRecord
                events.add(event)
            } catch (e: Exception) {
                logger.error("Failed to deserialize event: $eventJson", e)
            }
        }
        return events
    }

    private fun createConsumer(): KafkaConsumer<String, String> {
        val props = Properties()

        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name

        return KafkaConsumer<String, String>(props)
    }

    override fun close() {
        consumer.close()
    }
}
