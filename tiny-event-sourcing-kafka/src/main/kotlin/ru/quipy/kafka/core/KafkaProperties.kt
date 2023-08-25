package ru.quipy.kafka.core

/**
 * Configuration properties for Kafka.
 *
 * @property scanPublicAPIPackage the package name of public APIs to scan for @IntegrationMapper annotations.
 * @property bootstrapServers the comma-separated list of broker URLs in the host:port format.
 * @property replicationFactor the replication factor for created topics.
 * @property partitions the number of partitions for created topics.
 * @property acks the number of acknowledgments the producer requires for a record to be considered sent.
 * @property retries the number of times the producer will retry sending a record.
 * @property batchSize the maximum number of bytes the producer will include in a single batch.
 * @property lingerMs the amount of time the producer will wait before sending a batch, in milliseconds.
 * @property bufferMemory the total memory the producer can use to buffer records waiting to be sent to brokers.
 */
class KafkaProperties(
    var bootstrapServers: String? = null,
    var scanPublicAPIPackage: String? = null,
    var replicationFactor: Short = 1,
    var partitions: Int = 5,
    var acks: String = "all",
    var retries: Int = 3,
    var batchSize: Int = 16384,
    var lingerMs: Int = 1,
    var bufferMemory: Int = 33554432
)