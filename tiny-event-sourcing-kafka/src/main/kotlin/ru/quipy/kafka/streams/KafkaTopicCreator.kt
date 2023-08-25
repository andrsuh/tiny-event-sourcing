package ru.quipy.kafka.streams

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory

/**
 * [KafkaTopicCreator] provides functionality to create a Kafka topic if it doesn't already exist.
 */
class KafkaTopicCreator {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaTopicCreator::class.java)
    }

    fun createTopicIfNotExists(topicConfig: TopicConfig) {
        val newTopic = NewTopic(topicConfig.name, topicConfig.numPartitions, topicConfig.replicationFactor)
            .configs(topicConfig.config)

        val adminClientProps = mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to topicConfig.bootstrapServers
        )

        try {
            AdminClient.create(adminClientProps).use { adminClient ->
                if (!adminClient.listTopics().names().get().contains(topicConfig.name)) {
                    adminClient.createTopics(listOf(newTopic)).all().get()
                    logger.info("Topic created: {}", topicConfig.name)
                } else {
                    logger.info("Topic already exists: {}", topicConfig.name)
                }
            }
        } catch (e: Exception) {
            logger.error("Error while creating a topic: {}", topicConfig.name, e)
            throw RuntimeException("Error while creating a topic: ${topicConfig.name}", e)
        }
    }
}

/**
 * Represents the configuration of a Kafka topic.
 *
 * @property bootstrapServers The bootstrap servers for connecting to Kafka.
 * @property name The name of the topic.
 * @property numPartitions The number of partitions for the topic.
 * @property replicationFactor The replication factor for the topic.
 * @property config Additional configurations for the topic.
 */
data class TopicConfig(
    val bootstrapServers: String,
    val name: String,
    val numPartitions: Int,
    val replicationFactor: Short,
    val config: Map<String, String> = emptyMap()
)