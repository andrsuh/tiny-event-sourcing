package ru.quipy.kafka.streams

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.slf4j.LoggerFactory
import ru.quipy.domain.ExternalEventRecord
import ru.quipy.domain.Topic
import ru.quipy.kafka.core.KafkaProperties
import ru.quipy.streams.Producer
import java.time.Duration
import java.util.*

/**
 * [KafkaEventProducer] is a Kafka implementation of [Producer] interface.
 *
 * It is designed to produce [ExternalEventRecord]s to a Kafka topic. It uses a [KafkaProducer] internally.
 */

class KafkaEventProducer<T : Topic>(
    private val topicName: String,
    private val kafkaProperties: KafkaProperties
) : Producer<T> {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaEventProducer::class.java)
        private val objectMapper = ObjectMapper()
    }

    private val producer: KafkaProducer<String, String> = createProducer()

    init{
        producer.initTransactions()
        logger.info("Initiating transaction for $topicName")
    }

    override suspend fun sendEvents(partitionKey: String, externalEvents: List<ExternalEventRecord>) {
        try {
            producer.beginTransaction()
            logger.info("Transaction for $topicName topic with partition key $partitionKey started")

            for (externalEvent in externalEvents) {
                val record = ProducerRecord(
                    topicName,
                    partitionKey,
                    objectMapper.writeValueAsString(externalEvent)
                )
                producer.send(record)
            }

            producer.commitTransaction()
            logger.info("Transaction for $topicName topic with partition key $partitionKey committed")
        } catch (e: KafkaException) {
            producer.abortTransaction()
            logger.error("Transaction for $topicName topic with partition key $partitionKey aborted", e)
        } catch (e: Exception) {
            logger.error("An unexpected exception occurred: ${e.message}", e)
        }
    }

    private fun createProducer(): KafkaProducer<String, String> {
        val props = Properties()

        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        props[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true
        props[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = "transactional-id-config" + System.currentTimeMillis()
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"

        return KafkaProducer<String, String>(props)
    }

    override fun close() {
        producer.close(Duration.ofSeconds(10))
    }
}