package ru.quipy.mapper

import com.fasterxml.jackson.databind.ObjectMapper
import ru.quipy.domain.ExternalEvent
import ru.quipy.domain.Topic
import kotlin.reflect.KClass

interface ExternalEventMapper {

    fun <T : Topic> toEvent(payload: String, eventClass: KClass<out ExternalEvent<T>>): ExternalEvent<T>

    fun <T : Topic> eventToString(event: ExternalEvent<in T>): String
}

class JsonExternalEventMapper(
    val jsonObjectMapper: ObjectMapper
) : ExternalEventMapper {

    override fun <T : Topic> toEvent(payload: String, eventClass: KClass<out ExternalEvent<T>>): ExternalEvent<T> {
        return jsonObjectMapper.readValue(
            payload,
            eventClass.java
        ) as ExternalEvent<T>
    }

    override fun <T : Topic> eventToString(event: ExternalEvent<in T>): String {
        return jsonObjectMapper.writeValueAsString(event)
    }
}