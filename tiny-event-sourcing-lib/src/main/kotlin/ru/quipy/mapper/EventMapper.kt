package ru.quipy.mapper

import com.fasterxml.jackson.databind.ObjectMapper
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import kotlin.reflect.KClass

/**
 * Allows to map some row representation (json String by default) of event to instance of [Event] class and vise versa
 */
interface EventMapper {

    fun <A : Aggregate> toEvent(payload: String, eventClass: KClass<out Event<A>>): Event<A>

    fun <A : Aggregate> eventToString(event: Event<in A>): String
}

class JsonEventMapper(
    val jsonObjectMapper: ObjectMapper
) : EventMapper {

    override fun <A : Aggregate> toEvent(payload: String, eventClass: KClass<out Event<A>>): Event<A> {
        return jsonObjectMapper.readValue(
            payload,
            eventClass.java
        ) as Event<A>
    }

    override fun <A : Aggregate> eventToString(event: Event<in A>): String {
        return jsonObjectMapper.writeValueAsString(event)
    }
}