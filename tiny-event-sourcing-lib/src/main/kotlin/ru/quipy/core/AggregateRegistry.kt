package ru.quipy.core

import org.springframework.core.annotation.AnnotationUtils
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

/**
 * Acts as a local storage of the aggregates and their events meta-information.
 * Provides methods to store (register) this meta-information.
 * All the classes that are marked with [AggregateType] and extend [Aggregate] as well as those
 * marked with [DomainEvent] and extend [Event] should be explicitly registered here on app start up unless you're
 * using [SeekingForSuitableClassesAggregateRegistry]. [SeekingForSuitableClassesAggregateRegistry] decorates [BasicAggregateRegistry]
 * and provides automatic aggregates and domain events look up and registering.
 *
 * Used by many others libraries components to obtain necessary information about aggregates and events.
 *
 * For example [EventSourcingService] uses it to
 *  - obtain aggregate DB table name
 *  - get aggregate instantiation function (to create empty aggregate state) also [AggregateSubscriptionsManager] does that
 *  - to get event type by it's name retrieved from DB
 *
 */
interface AggregateRegistry {

    fun <A : Aggregate> register(
        clazz: KClass<in A>,
        eventRegistrationBlock: AggregateEventRegister<A>.() -> Unit
    )

    fun <A : Aggregate> getAggregateInfo(clazz: KClass<A>): AggregateInfo<A>?

    interface AggregateEventRegister<A : Aggregate> {
        fun <E : Event<A>> registerEvent(eventClass: KClass<E>)
    }

    /**
     * Encapsulates all the meta-information about aggregate and it's events that is needed by other library components.
     */
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