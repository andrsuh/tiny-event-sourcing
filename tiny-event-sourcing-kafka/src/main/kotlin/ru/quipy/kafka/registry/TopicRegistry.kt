package ru.quipy.kafka.registry

import org.springframework.core.annotation.AnnotationUtils
import ru.quipy.core.annotations.IntegrationEvent
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.Topic
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

/**
 * [TopicRegistry] is responsible for managing [Topic] registrations and [ExternalEvent]s
 * associated with those topics.
 */
interface TopicRegistry {

    fun <T : Topic> register(
        topicClass: KClass<in T>,
        eventRegistrationBlock: ExternalEventRegistrar<T>.() -> Unit
    )

    fun <T : Topic> basicTopicInfo(clazz: KClass<T>): BasicTopicInfo<T>?

    fun <T : Topic> getExternalEventInfo(clazz: KClass<T>): ExternalEventInfo<T>?

    interface ExternalEventRegistrar<T : Topic> {
        fun <E : ExternalEvent<T>> registerEvent(eventClass: KClass<E>)
    }

    interface BasicTopicInfo<T : Topic> {
        val topicClass: KClass<in T>
        val topicName: String
    }

    interface ExternalEventInfo<T : Topic> : BasicTopicInfo<T> {
        fun getExternalEventTypeByName(eventName: String): KClass<ExternalEvent<T>>
    }

    @Suppress("unused")
    class ExternalEventInfoImpl<T : Topic>(
        override val topicClass: KClass<in T>,
        override val topicName: String,
    ) : ExternalEventInfo<T>, ExternalEventRegistrar<T> {
        private val eventsMap: ConcurrentHashMap<String, KClass<ExternalEvent<T>>> = ConcurrentHashMap()

        override fun getExternalEventTypeByName(eventName: String): KClass<ExternalEvent<T>> {
            return eventsMap[eventName]
                ?: throw IllegalArgumentException("There is no such external event name $eventName for topic: ${topicClass.simpleName}")
        }

        override fun <E : ExternalEvent<T>> registerEvent(eventClass: KClass<E>) {
            val eventInfo = AnnotationUtils.findAnnotation(eventClass.java, IntegrationEvent::class.java)
                ?: throw IllegalStateException("No annotation ${IntegrationEvent::class.simpleName} provided on integration event ${eventClass.simpleName}")

            eventsMap.putIfAbsent(eventInfo.name, eventClass as KClass<ExternalEvent<T>>)?.also {
                throw IllegalStateException("External event ${eventInfo.name} already registered with class ${it.simpleName}")
            }
        }
    }
}