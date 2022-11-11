package ru.quipy.core

import org.springframework.core.annotation.AnnotationUtils
import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Aggregate
import ru.quipy.domain.AggregateState
import ru.quipy.domain.AggregateStateTransitionFunction
import ru.quipy.domain.Event
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

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
        aggregateClass: KClass<in A>,
        eventRegistrationBlock: EventRegistrar<A>.() -> Unit
    )

    fun <ID, A : Aggregate, S : AggregateState<ID, A>> register(
        aggregateClass: KClass<in A>,
        aggregateStateClass: KClass<in S>,
        eventRegistrationBlock: StateTransitionsRegistrar<ID, A, S>.() -> Unit
    )

    fun <A : Aggregate> getEventInfo(clazz: KClass<A>): EventInfo<A>?

    fun <ID, A : Aggregate, S : AggregateState<ID, A>> getStateTransitionInfo(clazz: KClass<A>): AggregateStateInfo<ID, A, S>?


    interface EventRegistrar<A : Aggregate> {
        fun <E : Event<A>> registerEvent(eventClass: KClass<E>)
    }

    interface StateTransitionsRegistrar<ID, A : Aggregate, S : AggregateState<ID, A>> {
        fun <E : Event<A>> registerStateTransition(
            eventClass: KClass<E>,
            eventStateTransitionFunction: AggregateStateTransitionFunction<A, E, S>
        )
    }

    interface BasicAggregateInfo<A : Aggregate> {
        val aggregateClass: KClass<in A>
        val aggregateEventsTableName: String
    }

    interface EventInfo<A : Aggregate> : BasicAggregateInfo<A> {
        fun getEventTypeByName(eventName: String): KClass<Event<A>>
    }

    interface AggregateStateInfo<ID, A : Aggregate, S : AggregateState<ID, A>> : EventInfo<A>, BasicAggregateInfo<A> {
        var emptyStateCreator: () -> S

        fun getStateTransitionFunction(eventName: String): AggregateStateTransitionFunction<A, Event<A>, S>
    }


    @Suppress("unused")
    class EventInfoImpl<A : Aggregate>(
        override val aggregateClass: KClass<in A>,
        override val aggregateEventsTableName: String,
    ) : EventInfo<A>, EventRegistrar<A> {
        private val eventsMap: ConcurrentHashMap<String, KClass<Event<A>>> = ConcurrentHashMap()

        override fun getEventTypeByName(eventName: String): KClass<Event<A>> {
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

    /**
     * Encapsulates all the meta-information about aggregate and it's events that is needed by other library components.
     */
    @Suppress("unused")
    class AggregateStateInfoImpl<ID, A : Aggregate, S : AggregateState<ID, A>>(
        override val aggregateClass: KClass<in A>,
        val aggregateStateClass: KClass<in S>,
        override val aggregateEventsTableName: String,
        private val eventsMap: ConcurrentHashMap<String, StateTransitionInfo<ID, A, S>> = ConcurrentHashMap(),
    ) : EventInfo<A>, AggregateStateInfo<ID, A, S>, StateTransitionsRegistrar<ID, A, S> {

        override lateinit var emptyStateCreator: () -> S

        override fun getEventTypeByName(eventName: String): KClass<Event<A>> {
            return (eventsMap[eventName]?.eventClass
                ?: throw IllegalArgumentException("There is no such event name $eventName for aggregate: ${aggregateClass.simpleName}")) as KClass<Event<A>>
        }

        // todo sukhoa bullshit with casts
        override fun getStateTransitionFunction(eventName: String): AggregateStateTransitionFunction<A, Event<A>, S> {
            return (eventsMap[eventName]?.transitionFunc
                ?: throw IllegalArgumentException("There is no such state transition function for event name $eventName for aggregate: ${aggregateClass.simpleName}")) as AggregateStateTransitionFunction<A, Event<A>, S>
        }

        override fun <E : Event<A>> registerStateTransition(
            eventClass: KClass<E>,
            eventStateTransitionFunction: AggregateStateTransitionFunction<A, E, S>
        ) {
            val eventInfo = AnnotationUtils.findAnnotation(eventClass.java, DomainEvent::class.java)
                ?: throw IllegalStateException("No annotation ${DomainEvent::class.simpleName} provided on domain event ${eventClass.simpleName}")

            val emptyStateCreatorFunction = aggregateStateClass.constructors.firstOrNull { it.parameters.isEmpty() }
                ?: throw IllegalStateException("No suitable empty constructor provided for aggregate state ${aggregateStateClass.simpleName}")

            emptyStateCreator = { emptyStateCreatorFunction.call() as S }

            eventsMap.putIfAbsent(eventInfo.name, StateTransitionInfo(eventClass, eventStateTransitionFunction))?.also {
                throw IllegalStateException("Event ${eventInfo.name} already registered with class ${eventClass.simpleName}")
            }
        }

        data class StateTransitionInfo<ID, A : Aggregate, S : AggregateState<ID, A>>(
            val eventClass: KClass<out Event<A>>,
            val transitionFunc: AggregateStateTransitionFunction<A, out Event<A>, S>,
        )
    }
}