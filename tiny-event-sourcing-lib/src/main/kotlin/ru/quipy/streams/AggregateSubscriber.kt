package ru.quipy.streams

import ru.quipy.domain.Aggregate
import kotlin.reflect.KClass

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class AggregateSubscriber(
    val aggregateClass: KClass<out Aggregate>,
    val subscriberName: String,
)