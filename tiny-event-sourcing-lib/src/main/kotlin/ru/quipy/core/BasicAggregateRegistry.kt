package ru.quipy.core

import org.springframework.core.annotation.AnnotationUtils
import ru.quipy.core.AggregateRegistry.*
import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate
import ru.quipy.domain.AggregateState
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

@Suppress("UNCHECKED_CAST")
class BasicAggregateRegistry : AggregateRegistry {
    private val aggregatesInfo = ConcurrentHashMap<KClass<*>, AggregateInfo>()

    override fun <A : Aggregate> register(
        aggregateClass: KClass<in A>,
        eventRegistrationBlock: EventRegistrar<A>.() -> Unit
    ){
        val aggregateInfo = AnnotationUtils.findAnnotation(aggregateClass.java, AggregateType::class.java)
            ?: throw IllegalStateException("No annotation ${aggregateClass.simpleName} provided on aggregate")

        aggregatesInfo.computeIfAbsent(aggregateClass) {
            AggregateInfo().also {
                it.eventInfo = EventInfoImpl(aggregateClass, aggregateInfo.aggregateEventsTableName)
            }
        }.also {
            eventRegistrationBlock(it.eventInfo as EventRegistrar<A>)
        }
    }

    override fun <ID, A : Aggregate, S : AggregateState<ID, A>> register(
        aggregateClass: KClass<in A>,
        aggregateStateClass: KClass<in S>,
        eventRegistrationBlock: StateTransitionsRegistrar<ID, A, S>.() -> Unit
    ) {
        val aggregateInfo = AnnotationUtils.findAnnotation(aggregateClass.java, AggregateType::class.java)
            ?: throw IllegalStateException("No annotation ${aggregateClass.simpleName} provided on aggregate")


        aggregatesInfo.computeIfAbsent(aggregateClass) {
            AggregateInfo().also {
                it.stateInfo = AggregateStateInfoImpl(aggregateClass, aggregateStateClass, aggregateInfo.aggregateEventsTableName)
            }
        }.also {
            eventRegistrationBlock(it.stateInfo as StateTransitionsRegistrar<ID, A, S>)
        }
    }

    override fun <A : Aggregate> getEventInfo(clazz: KClass<A>): EventInfo<A>? {
        return aggregatesInfo[clazz]?.let { (it.eventInfo ?: it.stateInfo) as EventInfo<A> }
    }

    override fun <ID, A : Aggregate, S: AggregateState<ID, A>> getStateTransitionInfo(clazz: KClass<A>): AggregateStateInfo<ID, A, S>? {
        return aggregatesInfo[clazz]?.let { it.stateInfo as AggregateStateInfo<ID, A, S> }
    }

    class AggregateInfo {
        var eventInfo: EventInfo<*>? = null
        var stateInfo: AggregateStateInfo<*, *, *>? = null
    }
}