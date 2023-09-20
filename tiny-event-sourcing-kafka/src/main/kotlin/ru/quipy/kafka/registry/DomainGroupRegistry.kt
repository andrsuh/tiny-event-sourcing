package ru.quipy.kafka.registry

import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.reflections.util.ClasspathHelper
import org.reflections.util.ConfigurationBuilder
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.kafka.core.DomainEventsGroup
import ru.quipy.kafka.core.KafkaProperties
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

/**
 * [DomainGroupRegistry] is responsible for managing the associations between domain event classes and
 * their corresponding [DomainEventsGroup] classes. It provides functionality to initialize the registry by
 * automatically scanning for domain event group classes and their associated domain event classes based on
 * annotations and package scanning.
 *
 * This registry allows to query and retrieve [DomainGroupRegistry] classes for a given domain event class
 * and retrieve a list of domain event classes associated with a specific [DomainEventsGroup] class.</p>
 */
class DomainGroupRegistry(
    private val kafkaProperties: KafkaProperties
) {

    private val internalGroups =
        mutableMapOf<
                KClass<out Event<*>>,
                KClass<DomainEventsGroup>
                >()

    private val internalEvents =
        mutableMapOf<
                KClass<out DomainEventsGroup>,
                MutableList<KClass<out Event<*>>>
                >()

    fun init() {
        val cfg = ConfigurationBuilder().addUrls(ClasspathHelper.forPackage(kafkaProperties.scanPublicAPIPackage))
            .addScanners(Scanners.TypesAnnotated, Scanners.SubTypes, Scanners.MethodsAnnotated)
        val refs = Reflections(cfg)

        val domainEventGroupAnnotatedClasses = refs.getSubTypesOf(DomainEventsGroup::class.java)
            .filter { it.isInterface && DomainEventsGroup::class.java.isAssignableFrom(it) }

        domainEventGroupAnnotatedClasses.forEach { domainEventGroupClass ->
            val domainEventGroupTypedClass = domainEventGroupClass.kotlin as KClass<DomainEventsGroup>
            val domainEventClasses = refs.getSubTypesOf(Event::class.java)
                .filter { it.kotlin.isSubclassOf(domainEventGroupTypedClass) ?: false }
                .map { it.kotlin } as List<KClass<out Event<*>>>
            domainEventClasses.forEach { domainEventClass ->
                if (internalGroups.containsKey(domainEventClass)) {
                    throw IllegalStateException("Duplicate event class found: ${domainEventClass.simpleName}")
                }

                internalGroups[domainEventClass] = domainEventGroupTypedClass
                val eventList = internalEvents.getOrPut(domainEventGroupTypedClass) { mutableListOf() }
                eventList.add(domainEventClass)
                logger.info("Added domain event class: ${domainEventClass.simpleName} to group: ${domainEventGroupTypedClass.simpleName}")
            }
        }


    }

    fun getGroupFromDomainEvent(eventClass: KClass<out Event<out Aggregate>>): KClass<out DomainEventsGroup>? {
        return internalGroups[eventClass]
    }

    fun getDomainEventsFromDomainGroup(domainGroupClass: KClass<out DomainEventsGroup>): List<KClass<out Event<out Aggregate>>> {
        return internalEvents[domainGroupClass] ?: emptyList()
    }
}