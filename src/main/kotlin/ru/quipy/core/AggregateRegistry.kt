package ru.quipy.core

import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import org.springframework.core.annotation.AnnotationUtils
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

@Suppress("UNCHECKED_CAST")
class AggregateRegistry {
    private val aggregatesMap = ConcurrentHashMap<KClass<*>, AggregateInfo<*>>()

    fun <A : Aggregate> register(
        clazz: KClass<in A>,
        eventRegistrationBlock: AggregateEventRegister<A>.() -> Unit
    ) {
        val aggregateInfo = AnnotationUtils.findAnnotation(clazz.java, AggregateType::class.java)
            ?: throw IllegalStateException("No aggregate ${clazz.simpleName} provided on aggregate")

        val constructor = clazz.constructors.firstOrNull {
            it.parameters.size == 1 && it.parameters[0].type.classifier == String::class
        }
            ?: throw IllegalStateException("No suitable constructor from ID class provided for aggregate ${clazz.simpleName}")


        aggregatesMap.computeIfAbsent(clazz) {
            AggregateInfo(clazz, aggregateInfo.aggregateEventsTableName, constructor)
        }.also {
            eventRegistrationBlock(it as AggregateEventRegister<A>)
        }
    }

    fun <A : Aggregate> getAggregateInfo(clazz: KClass<A>): AggregateInfo<A>? {
        return aggregatesMap[clazz]?.let { it as AggregateInfo<A> }
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
                ?: throw IllegalArgumentException("There is no such event type $eventName for aggregate: ${aggregateClass.simpleName}")
        }

        override fun <E: Event<A>> registerEvent(eventClass: KClass<E>) {
            val eventInfo = AnnotationUtils.findAnnotation(eventClass.java, DomainEvent::class.java)
                ?: throw IllegalStateException("No annotation ${DomainEvent::class.simpleName} provided on domain event ${eventClass.simpleName}")

            eventsMap.putIfAbsent(eventInfo.name, eventClass as KClass<Event<A>>)?.also {
                throw IllegalStateException("Event ${eventInfo.name} already registered with class ${it.simpleName}")
            }
        }
    }

    interface AggregateEventRegister<A : Aggregate> {
        fun <E: Event<A>> registerEvent(eventClass: KClass<E>)
    }
}