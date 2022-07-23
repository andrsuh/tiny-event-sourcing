package ru.quipy.core

import org.springframework.core.annotation.AnnotationUtils
import ru.quipy.core.AggregateRegistry.AggregateEventRegister
import ru.quipy.core.AggregateRegistry.AggregateInfo
import ru.quipy.domain.Aggregate
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

@Suppress("UNCHECKED_CAST")
class BasicAggregateRegistry: AggregateRegistry {
    private val aggregatesMap = ConcurrentHashMap<KClass<*>, AggregateInfo<*>>()

    override fun <A : Aggregate> register(
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

    override fun <A : Aggregate> getAggregateInfo(clazz: KClass<A>): AggregateInfo<A>? {
        return aggregatesMap[clazz]?.let { it as AggregateInfo<A> }
    }
}