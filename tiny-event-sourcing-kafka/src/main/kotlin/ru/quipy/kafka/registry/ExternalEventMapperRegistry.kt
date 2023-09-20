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
        val cfg = ConfigurationBuilder()
            .addUrls(ClasspathHelper.forPackage(kafkaProperties.scanPublicAPIPackage))
            .addScanners(Scanners.TypesAnnotated, Scanners.SubTypes, Scanners.MethodsAnnotated)
        val refs = Reflections(cfg)

        val externalEventsMappers = refs.getTypesAnnotatedWith(ExternalEventsMapper::class.java)

        externalEventsMappers.forEach { mapper ->
            val genericType = extractGenericType(mapper)
            val mapperClass = mapper.kotlin
            when {
                mapper.isAssignableFrom(DomainEventToExternalEventsMapper::class.java) -> {
                    val eventType = genericType as Class<Event<out Aggregate>>
                    if (oneToManyMappers.containsKey(eventType.kotlin)) {
                        throw IllegalStateException("Duplicate event ${eventType.simpleName} in mappers")
                    }
                    val oneToManyMapper =
                        mapperClass as KClass<DomainEventToExternalEventsMapper<Event<out Aggregate>>>
                    oneToManyMappers[eventType.kotlin] =
                        createMapper<DomainEventToExternalEventsMapper<Event<out Aggregate>>>(oneToManyMapper)
                    logger.info("Added domain event mapper: ${oneToManyMapper.simpleName} for event: ${eventType.simpleName}")
                }

                mapper.isAssignableFrom(DomainGroupToExternalEventsMapper::class.java) -> {
                    val domainGroupType = genericType as Class<DomainEventsGroup>
                    if (manyToManyMappers.containsKey(domainGroupType.kotlin)) {
                        throw IllegalStateException("Duplicate domain group: ${domainGroupType.simpleName} in mappers")
                    }
                    val manyToManyMapper = mapperClass as KClass<DomainGroupToExternalEventsMapper<DomainEventsGroup>>
                    manyToManyMappers[domainGroupType.kotlin] =
                        createMapper<DomainGroupToExternalEventsMapper<DomainEventsGroup>>(manyToManyMapper)
                    logger.info("Added domain event mapper: ${manyToManyMapper.simpleName} for group: ${domainGroupType.simpleName}")
                }
            }

        }
    }

    fun injectEventMappers(eventMappers: List<DomainEventToExternalEventsMapper<out Event<out Aggregate>>>) {
        for (mapper in eventMappers) {
            val eventType = extractEventType(mapper)

            if (oneToManyMappers.containsKey(eventType.kotlin)) {
                throw IllegalStateException("Duplicate event ${eventType.simpleName} in mappers")
            }

            oneToManyMappers[eventType.kotlin] = mapper
        }
    }

    fun injectGroupMappers(eventMappers: List<DomainGroupToExternalEventsMapper<out DomainEventsGroup>>) {
        for (mapper in eventMappers) {
            val groupType = extractGroupType(mapper)

            if (manyToManyMappers.containsKey(groupType.kotlin)) {
                throw IllegalStateException("Duplicate group ${groupType.simpleName} in mappers")
            }

            manyToManyMappers[groupType.kotlin] = mapper
        }
    }

    fun getOneToManyMapperFrom(eventClass: KClass<out Event<out Aggregate>>): DomainEventToExternalEventsMapper<Event<out Aggregate>>? {
        return oneToManyMappers[eventClass] as? DomainEventToExternalEventsMapper<Event<out Aggregate>>
    }

    fun getManyToManyMapperFrom(domainGroupClass: KClass<out DomainEventsGroup>): DomainGroupToExternalEventsMapper<DomainEventsGroup>? {
        return manyToManyMappers[domainGroupClass] as? DomainGroupToExternalEventsMapper<DomainEventsGroup>
    }

    private inline fun <reified T : Any> extractGenericType(mapper: Class<out T>): Class<*> {
        val interfaces = mapper::class.java.genericInterfaces
        if (interfaces.isNotEmpty() && interfaces[0] is ParameterizedType) {
            val genericInterface = interfaces[0] as ParameterizedType
            val genericType = genericInterface.actualTypeArguments[0]

            return genericType as Class<*>
        }
        throw IllegalArgumentException("Could not extract event type from mapper: ${mapper::class.qualifiedName}")
    }

    private fun extractEventType(mapper: DomainEventToExternalEventsMapper<out Event<out Aggregate>>): Class<out Event<out Aggregate>> {
        val interfaces = mapper::class.java.genericInterfaces
        if (interfaces.isNotEmpty() && interfaces[0] is ParameterizedType) {
            val genericInterface = interfaces[0] as ParameterizedType
            val genericType = genericInterface.actualTypeArguments[0]

            return genericType as Class<out Event<out Aggregate>>
        }
        throw IllegalArgumentException("Could not extract event type from mapper: ${mapper::class.qualifiedName}")
    }

    private fun extractGroupType(mapper: DomainGroupToExternalEventsMapper<out DomainEventsGroup>): Class<out DomainEventsGroup> {
        val interfaces = mapper::class.java.genericInterfaces
        if (interfaces.isNotEmpty() && interfaces[0] is ParameterizedType) {
            val genericInterface = interfaces[0] as ParameterizedType
            val genericType = genericInterface.actualTypeArguments[0]

            return genericType as Class<out DomainEventsGroup>
        }
        throw IllegalArgumentException("Could not extract event type from mapper: ${mapper::class.qualifiedName}")
    }

    private inline fun <reified T : Any> createMapper(mapperClass: KClass<out T>): T {
        return mapperClass.java.getDeclaredConstructor().newInstance()
    }
}
