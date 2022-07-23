package ru.quipy.core

import org.reflections.Reflections
import org.reflections.util.ClasspathHelper
import org.reflections.util.ConfigurationBuilder
import org.slf4j.LoggerFactory
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import kotlin.math.log
import kotlin.reflect.KClass


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
//        .filterInputsBy(FilterBuilder().excludePackage(""))

        val refs = Reflections(cfg)
        val aggregates = refs.getSubTypesOf(Aggregate::class.java).map { it.kotlin }
        val events = refs.getTypesAnnotatedWith(DomainEvent::class.java).map {
            val eventClass = it
            var c = it
            while (c.superclass != Event::class.java) {
                c = c.superclass
            }
            c.kotlin.supertypes[0].arguments[0].type!!.classifier as KClass<Aggregate> to eventClass.kotlin as KClass<Event<Nothing>>
        }.groupBy({ it.first }) { it.second }


        aggregates.forEach { aggregateClass ->
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