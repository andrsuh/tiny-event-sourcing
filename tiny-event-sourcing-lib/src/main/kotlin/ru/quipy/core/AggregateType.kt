package ru.quipy.core

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class AggregateType(
    val aggregateEventsTableName: String,
)
