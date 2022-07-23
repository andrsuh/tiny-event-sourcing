package ru.quipy.core

import org.springframework.core.annotation.AnnotationUtils
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

interface AggregateRegistry {

    fun <A : Aggregate> register(
        clazz: KClass<in A>,
        eventRegistrationBlock: AggregateEventRegister<A>.() -> Unit
    )

    fun <A : Aggregate> getAggregateInfo(clazz: KClass<A>): AggregateInfo<A>?

    interface AggregateEventRegister<A : Aggregate> {
        fun <E : Event<A>> registerEvent(eventClass: KClass<E>)
    }

    @Suppress("unused")
    class AggregateInfo<A : Aggregate>(
        val aggregateClass: KClass<in A>,
        val aggregateEventsTableName: String,
        constructor: KFunction<Any?>,
        val instantiateFunction: (String) -> A = { constructor.call(it) as A },
        private val eventsMap: ConcurrentHashMap<String, KClass<Event<A>>> = ConcurrentHashMap()
    ) : AggregateEventRegister<A> {
        fun getEventTypeByName(eventName: String): KClass<Event<A>> {
            return eventsMap[eventName]
                ?: throw IllegalArgumentException("There is no such event name $eventName for aggregate: ${aggregateClass.simpleName}")
        }

        override fun <E : Event<A>> registerEvent(eventClass: KClass<E>) {
            val eventInfo = AnnotationUtils.findAnnotation(eventClass.java, DomainEvent::class.java)
                ?: throw IllegalStateException("No annotation ${DomainEvent::class.simpleName} provided on domain event ${eventClass.simpleName}")

            eventsMap.putIfAbsent(eventInfo.name, eventClass as KClass<Event<A>>)?.also {
                throw IllegalStateException("Event ${eventInfo.name} already registered with class ${it.simpleName}")
            }
        }
    }
}