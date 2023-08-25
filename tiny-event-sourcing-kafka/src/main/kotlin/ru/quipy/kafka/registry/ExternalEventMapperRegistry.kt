package ru.quipy.kafka.registry

import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.reflections.util.ClasspathHelper
import org.reflections.util.ConfigurationBuilder
import ru.quipy.core.annotations.ExternalEventsMapper
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.kafka.core.DomainEventToExternalEventsMapper
import ru.quipy.kafka.core.DomainEventsGroup
import ru.quipy.kafka.core.DomainGroupToExternalEventsMapper
import ru.quipy.kafka.core.KafkaProperties
import java.lang.reflect.ParameterizedType
import kotlin.reflect.KClass

class ExternalEventMapperRegistry(private val kafkaProperties: KafkaProperties) {

    private val oneToManyMappers = mutableMapOf<KClass<out Event<out Aggregate>>, KClass<out DomainEventToExternalEventsMapper<*>>>()
    private val manyToManyMappers = mutableMapOf<KClass<out DomainEventsGroup>, KClass<out DomainGroupToExternalEventsMapper<*>>>()

    fun init() {
        val cfg = ConfigurationBuilder()
            .addUrls(ClasspathHelper.forPackage(kafkaProperties.scanPublicAPIPackage))
            .addScanners(Scanners.TypesAnnotated, Scanners.SubTypes, Scanners.MethodsAnnotated)
        val refs = Reflections(cfg)

        val externalEventsMappers = refs.getTypesAnnotatedWith(ExternalEventsMapper::class.java)

        externalEventsMappers.forEach { mapper ->
            val interfaces = mapper.genericInterfaces
            if (interfaces.isNotEmpty() && interfaces[0] is ParameterizedType) {
                val genericInterface = interfaces[0] as ParameterizedType
                val genericType = genericInterface.actualTypeArguments[0]
                val mapperClass = mapper.kotlin

                if (DomainEventToExternalEventsMapper::class.java.isAssignableFrom(mapper)) {
                    val eventType = genericType as Class<out Event<out Aggregate>>
                    if (oneToManyMappers.containsKey(eventType.kotlin)) {
                        throw RuntimeException("Duplicate event ${eventType.simpleName} in mappers")
                    }
                    oneToManyMappers[eventType.kotlin] = mapperClass as KClass<DomainEventToExternalEventsMapper<*>>
                } else if (DomainGroupToExternalEventsMapper::class.java.isAssignableFrom(mapper)) {
                    val domainGroupType = genericType as Class<DomainEventsGroup>
                    if (manyToManyMappers.containsKey(domainGroupType.kotlin)) {
                        throw RuntimeException("Duplicate domain group: ${domainGroupType.simpleName} in mappers")
                    }
                    manyToManyMappers[domainGroupType.kotlin] = mapperClass as KClass<DomainGroupToExternalEventsMapper<*>>
                }
            }
        }
    }

    fun <E : Event<out Aggregate>> getOneToManyMapperFrom(eventClass: KClass<E>): KClass<DomainEventToExternalEventsMapper<E>> {
        return oneToManyMappers[eventClass] as KClass<DomainEventToExternalEventsMapper<E>>
    }

    fun <G : DomainEventsGroup> getManyToManyMapperFrom(domainGroupClass: KClass<G>): KClass<DomainGroupToExternalEventsMapper<G>> {
        return manyToManyMappers[domainGroupClass] as KClass<DomainGroupToExternalEventsMapper<G>>
    }
}


