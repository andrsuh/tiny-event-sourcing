package ru.quipy.kafka.streams

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import ru.quipy.domain.ExternalEventRecord
import ru.quipy.domain.Topic
import ru.quipy.kafka.core.KafkaProperties
import ru.quipy.streams.Producer
import java.time.Duration
import java.util.*

class KafkaEventProducer <T : Topic>(
    private val topicName: String,
    private val kafkaProperties: KafkaProperties
) : Producer <T> {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaEventProducer::class.java)
        private val objectMapper = ObjectMapper()
    }

    private val producer: KafkaProducer<String, String> = createProducer()

//    private val topicName = topicEntityClass.findAnnotation<TopicType>()?.name

    override suspend fun sendEvents(partitionKey: String, externalEvents: List<ExternalEventRecord>) {
        for (externalEvent in externalEvents) {
            val record = ProducerRecord(
                topicName,
                partitionKey,
                objectMapper.writeValueAsString(externalEvent)
            )
            producer.send(record) { metadata, exception ->
                if (exception != null) {
                    logger.error(
                        "Error sending event $externalEvent to topic $topicName",
                        exception
                    )
                } else {
                    logger.debug(
                        "Sent event {} to topic {}, partition {}, offset {}",
                        externalEvent,
                        topicName,
                        metadata.partition(),
                        metadata.offset()
                    )
                }
            }
        }
    }

    private fun createProducer(): KafkaProducer<String, String> {
        val props = Properties()

        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        return KafkaProducer<String, String>(props)
    }

    override fun close() {
        producer.close(Duration.ofSeconds(10))
    }
}