package ru.quipy.core.annotations

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class AggregateType(
    val aggregateEventsTableName: String,
)
