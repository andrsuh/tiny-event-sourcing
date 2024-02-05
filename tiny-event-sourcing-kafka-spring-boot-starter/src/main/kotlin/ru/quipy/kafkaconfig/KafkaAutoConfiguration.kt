package ru.quipy.kafkaconfig

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.database.OngoingGroupStorage
import ru.quipy.kafka.core.KafkaProperties
import ru.quipy.kafka.core.OngoingGroupManager
import ru.quipy.kafka.registry.*
import ru.quipy.kafka.streams.KafkaTopicCreator
import ru.quipy.kafka.streams.TopicEventStreamManager
import ru.quipy.kafka.streams.TopicSubscriptionsManager
import ru.quipy.mapper.EventMapper
import ru.quipy.mapper.ExternalEventMapper
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.mapper.JsonExternalEventMapper
import ru.quipy.streams.AggregateEventStreamManager

@Configuration
class KafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun eventMapper(jsonObjectMapper: ObjectMapper) = JsonEventMapper(jsonObjectMapper)


    @Bean
    @ConditionalOnMissingBean
    fun externalEventMapper(jsonObjectMapper: ObjectMapper) = JsonExternalEventMapper(jsonObjectMapper)

    @Bean
    @ConfigurationProperties(prefix = "kafka")
    @ConditionalOnMissingBean
    fun kafkaProperties() = KafkaProperties()

    @Bean(initMethod = "init")
    @ConditionalOnMissingBean
    fun topicRegistry(kafkaProperties: KafkaProperties) =
        SeekingForSuitableClassesTopicRegistry(BasicTopicRegistry(), kafkaProperties)

    @Bean(initMethod = "init")
    @ConditionalOnMissingBean
    fun groupRegistry(kafkaProperties: KafkaProperties, ongoingGroupStorage: OngoingGroupStorage) = DomainGroupRegistry(kafkaProperties)

    @Bean(initMethod = "init")
    @ConditionalOnMissingBean
    fun externalEventMapperRegistry(kafkaProperties: KafkaProperties) = ExternalEventMapperRegistry(kafkaProperties)

    @Bean
    @ConditionalOnMissingBean
    fun topicEventStreamManager(
        topicRegistry: TopicRegistry,
        eventSourcingProperties: EventSourcingProperties,
        kafkaProperties: KafkaProperties
    ) = TopicEventStreamManager(
        topicRegistry,
        eventSourcingProperties,
        kafkaProperties
    )

    @Bean
    @ConditionalOnMissingBean
    fun kafkaTopicCreator() = KafkaTopicCreator()

    @Bean
    @ConditionalOnMissingBean
    fun ongoingGroupManager(
        groupRegistry: DomainGroupRegistry,
        ongoingGroupStorage: OngoingGroupStorage,
        externalEventMapperRegistry: ExternalEventMapperRegistry,
        eventMapper: EventMapper,
        externalMapper: ExternalEventMapper
    ) = OngoingGroupManager(groupRegistry, ongoingGroupStorage, externalEventMapperRegistry, eventMapper, externalMapper)

    @Bean
    @ConditionalOnBean(OngoingGroupStorage::class)
    @ConditionalOnMissingBean
    fun topicSubscriptionsManager(
        aggregateEventStreamManager: AggregateEventStreamManager,
        topicEventStreamManager: TopicEventStreamManager,
        aggregateRegistry: AggregateRegistry,
        topicRegistry: TopicRegistry,
        eventMapper: JsonEventMapper,
        externalEventMapper: JsonExternalEventMapper,
        kafkaProperties: KafkaProperties,
        groupRegistry: DomainGroupRegistry,
        kafkaTopicCreator: KafkaTopicCreator,
        ongoingGroupManager: OngoingGroupManager
    ) = TopicSubscriptionsManager(
        aggregateEventStreamManager,
        topicEventStreamManager,
        aggregateRegistry,
        topicRegistry,
        eventMapper,
        externalEventMapper,
        kafkaProperties,
        groupRegistry,
        kafkaTopicCreator,
        ongoingGroupManager
    )
}