package ru.quipy.kafka.registry

import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.reflections.util.ClasspathHelper
import org.reflections.util.ConfigurationBuilder
import org.slf4j.LoggerFactory
import ru.quipy.core.annotations.IntegrationEvent
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.Topic
import ru.quipy.kafka.core.KafkaProperties
import kotlin.reflect.KClass


class SeekingForSuitableClassesTopicRegistry(
    private val wrappedRegistry: TopicRegistry,
    private val kafkaProperties: KafkaProperties
) : TopicRegistry by wrappedRegistry {

    private val logger = LoggerFactory.getLogger(SeekingForSuitableClassesTopicRegistry::class.java)

    public fun init() {
        if (kafkaProperties.scanPublicAPIPackage.isNullOrBlank()) {
            logger.error("Scanning package is not set while automatic scanning for topics and external events enabled. Set auth scan property")
            throw IllegalStateException("You haven't specify 'scanPackage' property. Please set it to the package that need to be scanned for topics and external events.")
        }

        val cfg = ConfigurationBuilder()
            .addUrls(ClasspathHelper.forPackage(kafkaProperties.scanPublicAPIPackage))   // todo sukhoa we can scan many packages
            .addScanners(Scanners.TypesAnnotated, Scanners.SubTypes, Scanners.MethodsAnnotated)

        val refs = Reflections(cfg)

        // search for all the candidates to the Topic
        val topics = refs.getSubTypesOf(Topic::class.java).map { it.kotlin as KClass<Topic> }

        // search for all the candidates to the IntegrationEvent
        // map topic class to all of its events
        val events = refs.getTypesAnnotatedWith(IntegrationEvent::class.java).map {
            val eventClass = it
            var c = it
            while (c.superclass != ExternalEvent::class.java) { // todo sukhoa is it possible to get infinite loop
                c = c.superclass
            }
            // class might have more than one superclass
            c.kotlin.supertypes.find { superType -> superType.classifier == ExternalEvent::class }!!.arguments[0].type!!.classifier as KClass<Topic> to eventClass.kotlin as KClass<ExternalEvent<Topic>>
        }.groupBy({ it.first }) { it.second }

        // bring it all together
        topics.forEach { topic ->
            wrappedRegistry.register(topic) {
                events[topic]?.forEach { eventClass ->
                    registerEvent(eventClass)
                    logger.info("Topic : ${topic.simpleName}, event : ${eventClass.simpleName} registered")
                }
                events[topic]?.also {
                    logger.info("Registered ${it.size} events for topic: ${topic.simpleName}")
                }
            }
        }
    }
}