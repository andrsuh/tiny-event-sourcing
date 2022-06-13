package ru.quipy.streams.annotation

import ru.quipy.domain.Aggregate
import kotlin.reflect.KClass

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class AggregateSubscriber(
    val aggregateClass: KClass<out Aggregate>,
    val subscriberName: String,
    val retry: RetryConf = RetryConf(3, RetryFailedStrategy.SKIP_EVENT)
)

annotation class RetryConf(
    val maxAttempts: Int,
    val lastAttemptFailedStrategy: RetryFailedStrategy
)

/**
 * In case event handling failure (when subscriber failed to process it correct - exception or just returning false for some reason)
 * we can choose between two strategies:
 * - suspend the whole stream and wait till the situation is soled manually somehow
 * - skip the event and take the next one sacrificing the projection data consistency but continuing pocessing events
 */
enum class RetryFailedStrategy {
    SUSPEND,
    SKIP_EVENT
}
