package ru.quipy.mapper

import com.fasterxml.jackson.databind.ObjectMapper
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import kotlin.reflect.KClass

interface EventMapper {

    fun <A : Aggregate> toEvent(payload: String, eventClass: KClass<out Event<*>>): Event<A>

    fun <A : Aggregate> eventToString(event: Event<in A>): String
}

class JsonEventMapper(
    val jsonObjectMapper: ObjectMapper
) : EventMapper {

    override fun <A : Aggregate> toEvent(
        payload: String,
        eventClass: KClass<out Event<*>>
    ): Event<A> {
        return jsonObjectMapper.readValue(
            payload,
            eventClass.java
        ) as Event<A>
    }

    override fun <A : Aggregate> eventToString(event: Event<in A>): String {
        return jsonObjectMapper.writeValueAsString(event)
    }
}