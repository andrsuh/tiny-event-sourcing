package ru.quipy.kafka.registry

import org.springframework.core.annotation.AnnotationUtils
import ru.quipy.core.annotations.TopicType
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.Topic
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

/**
 * [BasicTopicRegistry] is an implementation of the [TopicRegistry] that
 * allows for the registration of [Topic]s and their associated [ExternalEvent]'s.
 *
 * It maintains a registry of topics and their related information, such as the name and external events.
 */
@Suppress("UNCHECKED_CAST")
class BasicTopicRegistry : TopicRegistry {
    private val topicInfo = ConcurrentHashMap<KClass<*>, TopicInfo>()

    override fun <T : Topic> register(
        topicClass: KClass<in T>,
        eventRegistrationBlock: TopicRegistry.ExternalEventRegistrar<T>.() -> Unit
    ) {
        val topicInfo = AnnotationUtils.findAnnotation(topicClass.java, TopicType::class.java)
            ?: throw IllegalStateException("No annotation ${topicClass.simpleName} provided on topic")

        this.topicInfo.computeIfAbsent(topicClass) {
            TopicInfo().also {
                it.eventInfo = TopicRegistry.ExternalEventInfoImpl(topicClass, topicInfo.name)
            }
        }.also {
            eventRegistrationBlock(it.eventInfo as TopicRegistry.ExternalEventRegistrar<T>)
        }
    }

    override fun <T : Topic> basicTopicInfo(clazz: KClass<T>): TopicRegistry.BasicTopicInfo<T>? {
        return topicInfo[clazz]
            ?.let {
                (it.eventInfo)
                        as TopicRegistry.BasicTopicInfo<T>
            }
    }

    override fun <T : Topic> getExternalEventInfo(clazz: KClass<T>): TopicRegistry.ExternalEventInfo<T>? {
        return topicInfo[clazz]
            ?.let {
                (it.eventInfo)
                        as TopicRegistry.ExternalEventInfo<T>
            }
    }

    class TopicInfo {
        var eventInfo: TopicRegistry.ExternalEventInfo<*>? = null
        //group, mapper ???
    }
}