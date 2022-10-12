package ru.quipy.core

import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.reflections.util.ClasspathHelper
import org.reflections.util.ConfigurationBuilder
import org.slf4j.LoggerFactory
import ru.quipy.core.annotations.DomainEvent
import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.Aggregate
import ru.quipy.domain.AggregateState
import ru.quipy.domain.Event
import java.lang.reflect.Modifier
import kotlin.reflect.KClass
import kotlin.reflect.full.isSuperclassOf


class SeekingForSuitableClassesAggregateRegistry(
    private val wrappedRegistry: AggregateRegistry,
    private val eventSourcingProperties: EventSourcingProperties
) :
    AggregateRegistry by wrappedRegistry {

    private val logger = LoggerFactory.getLogger(SeekingForSuitableClassesAggregateRegistry::class.java)

    public fun init() {
        if (!eventSourcingProperties.autoScanEnabled) {
            logger.warn("Skipping automatic scanning for aggregates and events. You should register them manually in Aggregate Registry")
            return
        }

        if (eventSourcingProperties.scanPackage.isNullOrBlank()) {
            logger.error("Scanning package is not set while automatic scanning for aggregates and events enabled. Set auth scan property")
            return
        }

        val cfg = ConfigurationBuilder()
            .addUrls(ClasspathHelper.forPackage(eventSourcingProperties.scanPackage))   // todo sukhoa we can scan many packages
            .addScanners(Scanners.TypesAnnotated, Scanners.SubTypes, Scanners.MethodsAnnotated)
//        .filterInputsBy(FilterBuilder().excludePackage(""))

        val refs = Reflections(cfg)

        // search for all the candidates to the Aggregate
        val aggregates = refs.getSubTypesOf(Aggregate::class.java).map { it.kotlin as KClass<Aggregate> }

        // search for all the candidates to the AggregateState and map the aggregate class to the corresponding state
        val aggregatesStates = refs.getSubTypesOf(AggregateState::class.java).associate {
            it.kotlin.supertypes[0].arguments[1].type!!.classifier as KClass<Aggregate> to it.kotlin as KClass<AggregateState<Any, Aggregate>>
        } // todo sukhoa here we can perform validation that there is only one state for aggregate

        // search for all the candidates to the DomainEvent
        // map aggregate class to all of its events
        val events = refs.getTypesAnnotatedWith(DomainEvent::class.java).map {
            val eventClass = it
            var c = it
            while (c.superclass != Event::class.java) { // todo sukhoa is it possible to get infinite loop
                c = c.superclass
            }
            c.kotlin.supertypes[0].arguments[0].type!!.classifier as KClass<Aggregate> to eventClass.kotlin as KClass<Event<Aggregate>>
        }.groupBy({ it.first }) { it.second }

        // search for all the candidates to the aggregate state transition functions (static and not)
        // map combination of aggregate state and the event to the function that is responsible for this transition
        val stateFunctions = refs.getMethodsAnnotatedWith(StateTransitionFunc::class.java)
            .filter { method ->
                val paramNum = method.parameters.size
//                 todo sukhoa here the log what was filtered out
                if (Modifier.isStatic(method.modifiers)) {
                    paramNum == 2
                            && AggregateState::class.isSuperclassOf(method.parameters[0].type.kotlin)
                            && Event::class.isSuperclassOf(method.parameters[1].type.kotlin)
                } else {
                    AggregateState::class.isSuperclassOf(method.declaringClass.kotlin)
                            && paramNum == 1
                            && Event::class.isSuperclassOf(method.parameters[0].type.kotlin)
                }
            }.map { method ->
                // as AggregateStateTransitionFunction<Aggregate, Event<Aggregate>, AggregateState<Any, Aggregate>>
                val stateClass: KClass<*>
                val eventClass: KClass<*>
                if (Modifier.isStatic(method.modifiers)) {
                    stateClass = method.parameters[0].type.kotlin
                    eventClass = method.parameters[1].type.kotlin
                } else {
                    stateClass = method.declaringClass.kotlin
                    eventClass = method.parameters[0].type.kotlin
                }
                (stateClass as KClass<AggregateState<Any, Aggregate>> to eventClass as KClass<Event<Aggregate>>) to method
            }.toMap()

        // bring it all together
        aggregates.forEach { aggregateClass ->
            val stateClass = aggregatesStates[aggregateClass]
            if (stateClass != null) {
                wrappedRegistry.register(aggregateClass, stateClass) {
                    events[aggregateClass]?.forEach { eventClass ->


                        val transitionFunction = stateFunctions[(stateClass to eventClass)]
                            ?: throw IllegalStateException(
                                "Error during registering aggregate ${aggregateClass.simpleName}. " +
                                        "There is no state transition function found for state ${stateClass.simpleName} and event ${eventClass.simpleName}"
                            )
                        val static = Modifier.isStatic(transitionFunction.modifiers)

                        this.registerStateTransition(eventClass) { state, event ->
                            if (static) {
                                transitionFunction.invoke(null, state, event)
                            } else {
                                transitionFunction.invoke(state, event)
                            }
                        }
                        logger.info("Aggregate : ${aggregateClass.simpleName}, event : ${eventClass.simpleName} registered")
                    }
                    events[aggregateClass]?.also {
                        logger.info("Registered ${it.size} events for aggregate: ${aggregateClass.simpleName}")
                    }
                }
            } else {
                // todo sukhoa log here that there is no state class
                wrappedRegistry.register(aggregateClass) {
                    events[aggregateClass]?.forEach { eventClass ->
                        registerEvent(eventClass)
                        logger.info("Aggregate : ${aggregateClass.simpleName}, event : ${eventClass.simpleName} registered")
                    }
                    events[aggregateClass]?.also {
                        logger.info("Registered ${it.size} events for aggregate: ${aggregateClass.simpleName}")
                    }
                }
            }
        }
    }
}