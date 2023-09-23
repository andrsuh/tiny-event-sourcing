package ru.quipy.kafka.registry

import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.reflections.util.ClasspathHelper
import org.reflections.util.ConfigurationBuilder
import org.slf4j.LoggerFactory
import ru.quipy.core.annotations.ExternalEventsMapper
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.kafka.core.DomainEventToExternalEventsMapper
import ru.quipy.kafka.core.DomainEventsGroup
import ru.quipy.kafka.core.DomainGroupToExternalEventsMapper
import ru.quipy.kafka.core.KafkaProperties
import java.lang.reflect.ParameterizedType
import kotlin.reflect.KClass

/**
 * [ExternalEventMapperRegistry] is responsible for managing the associations between domain event
 * classes, domain event group classes, and their corresponding external event mapper classes. It provides
 * functionality to initialize the registry by automatically scanning for mappers annotated with
 * [ExternalEventsMapper] and mapping them to the appropriate domain events or domain event groups.
 *
 * This registry allows to query and retrieve the external event mapper classes for a given domain event or
 * domain event group class.
 */
class ExternalEventMapperRegistry(
    private val kafkaProperties: KafkaProperties
) {

    private val logger = LoggerFactory.getLogger(ExternalEventMapperRegistry::class.java)

    private val oneToManyMappers =
        mutableMapOf<KClass<out Event<out Aggregate>>, DomainEventToExternalEventsMapper<out Event<out Aggregate>>>()
    private val manyToManyMappers =
        mutableMapOf<KClass<out DomainEventsGroup>, DomainGroupToExternalEventsMapper<out DomainEventsGroup>>()

    private fun init() {
        val cfg = ConfigurationBuilder().addUrls(ClasspathHelper.forPackage(kafkaProperties.scanPublicAPIPackage))
            .addScanners(Scanners.TypesAnnotated, Scanners.SubTypes, Scanners.MethodsAnnotated)
        val refs = Reflections(cfg)

        val externalEventsMappers = refs.getTypesAnnotatedWith(ExternalEventsMapper::class.java)

        externalEventsMappers.forEach { mapperClass ->
            if (DomainEventToExternalEventsMapper::class.java.isAssignableFrom(mapperClass)) {
                extractEventMapper(mapperClass)
            } else if (DomainGroupToExternalEventsMapper::class.java.isAssignableFrom(mapperClass)) {
                extractGroupMapper(mapperClass)
            }
        }
    }

    private fun extractEventMapper(mapperClass: Class<*>) {
        val eventType = extractGenericType(mapperClass).kotlin as KClass<out Event<out Aggregate>>
        if (oneToManyMappers.containsKey(eventType)) {
            throw IllegalStateException("Duplicate event ${eventType.simpleName} in mappers")
        }
        val oneToManyMapper = mapperClass.kotlin as KClass<DomainEventToExternalEventsMapper<Event<out Aggregate>>>
        oneToManyMappers[eventType] =
            createMapper<DomainEventToExternalEventsMapper<Event<out Aggregate>>>(oneToManyMapper)
        logger.info("Domain event mapper: ${oneToManyMapper.simpleName} for event: ${eventType.simpleName} registered")
    }

    private fun extractGroupMapper(mapperClass: Class<*>) {
        val domainGroupType = extractGenericType(mapperClass).kotlin as KClass<out DomainEventsGroup>
        if (manyToManyMappers.containsKey(domainGroupType)) {
            throw IllegalStateException("Duplicate domain group: ${domainGroupType.simpleName} in mappers")
        }
        val manyToManyMapper = mapperClass.kotlin as KClass<DomainGroupToExternalEventsMapper<DomainEventsGroup>>
        manyToManyMappers[domainGroupType] =
            createMapper<DomainGroupToExternalEventsMapper<DomainEventsGroup>>(manyToManyMapper)
        logger.info("Domain group mapper: ${manyToManyMapper.simpleName} for group: ${domainGroupType.simpleName} registered")
    }

    fun injectEventMappers(eventMappers: List<KClass<out DomainEventToExternalEventsMapper<out Event<out Aggregate>>>>) {
        for (mapper in eventMappers) {
            extractEventMapper(mapper.java)
        }
    }

    fun injectGroupMappers(eventMappers: List<KClass<out DomainGroupToExternalEventsMapper<out DomainEventsGroup>>>) {
        for (mapper in eventMappers) {
            extractGroupMapper(mapper.java)
        }
    }

    fun getOneToManyMapperFrom(eventClass: KClass<out Event<out Aggregate>>): DomainEventToExternalEventsMapper<Event<out Aggregate>>? {
        return oneToManyMappers[eventClass] as? DomainEventToExternalEventsMapper<Event<out Aggregate>>
    }

    fun getManyToManyMapperFrom(domainGroupClass: KClass<out DomainEventsGroup>): DomainGroupToExternalEventsMapper<DomainEventsGroup>? {
        return manyToManyMappers[domainGroupClass] as? DomainGroupToExternalEventsMapper<DomainEventsGroup>
    }

    private inline fun <reified T : Any> extractGenericType(mapper: Class<out T>): Class<*> {
        val interfaces = mapper.genericInterfaces
        if (interfaces.isNotEmpty() && interfaces[0] is ParameterizedType) {
            val genericInterface = interfaces[0] as ParameterizedType
            val genericType = genericInterface.actualTypeArguments[0]

            return genericType as Class<*>
        }
        throw IllegalArgumentException("Could not extract event type from mapper: ${mapper::class.qualifiedName}")
    }

    private inline fun <reified T : Any> createMapper(mapperClass: KClass<out T>): T {
        return mapperClass.java.getDeclaredConstructor().newInstance()
    }
}
