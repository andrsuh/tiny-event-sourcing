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

class DomainGroupRegistry(
    private val kafkaProperties: KafkaProperties
) {

    private val internalGroups =
        mutableMapOf<
                KClass<out Event<*>>,
                KClass<DomainEventsGroup>
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
                    throw RuntimeException("Duplicate event class found: ${domainEventClass.simpleName}")
                }
                internalGroups[domainEventClass] = domainEventGroupTypedClass
            }
        }


    }

    fun <E : Event<out Aggregate>> getGroupFromDomainEvent(eventClass: KClass<E>): KClass<DomainEventsGroup>? {
        return internalGroups[eventClass]
    }

    fun <G : DomainEventsGroup> getDomainEventsFromDomainGroup(domainGroupClass: KClass<G>): List<KClass<out Event<out Aggregate>>> {
        return internalGroups.filterValues { it == domainGroupClass }
            .keys.toList()
    }
}