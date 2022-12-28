package ru.quipy.streams

import org.slf4j.LoggerFactory
import org.springframework.core.annotation.AnnotationUtils
import ru.quipy.core.AggregateRegistry
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.mapper.EventMapper
import ru.quipy.streams.EventStreamSubscriber.EventStreamSubscriptionBuilder
import ru.quipy.streams.annotation.AggregateSubscriber
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import ru.quipy.streams.annotation.SubscribeEvent
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotations
import kotlin.reflect.full.isSuperclassOf
import kotlin.reflect.full.memberFunctions

/**
 * Creates [EventStreamSubscriber]s holds them and allows to destroy them all.
 *
 * Using this class is a preferable way to create and initialize [EventStreamSubscriber]s.
 *
 * There are two options to do that:
 * - Pass some [Any] object of class that is marked with [AggregateSubscriber] to [AggregateSubscriptionsManager.subscribe].
 * [AggregateSubscriptionsManager] will create a subscriber for the class.
 * Also, the class will be automatically scanned to find methods that are marked with [SubscribeEvent] and create handlers
 * for subscriber to deal with events of type that matches the argument type of the method.
 * - Use [AggregateSubscriptionsManager.createSubscriber] method which alloes you to instantiate subscriber with
 * explicitly passed config and configure handlers.
 */
class AggregateSubscriptionsManager(
    private val eventsStreamManager: AggregateEventStreamManager,
    private val aggregateRegistry: AggregateRegistry,
    private val eventMapper: EventMapper
) {
    private val logger = LoggerFactory.getLogger(AggregateSubscriptionsManager::class.java)

    private val subscribers: MutableList<EventStreamSubscriber<*>> = mutableListOf()

    /**
     * Subscribes given object to the aggregate event stream.
     *
     * Automatically scans the class
     * - Looking for [AggregateSubscriber] annotation. If not found throw the exception.
     * - Looking for methods in class that are marked with [SubscribeEvent] and analyses its arguments that should be a
     * type of aggregate [Event].
     * - Creates the subscriber of the aggregate and configure the corresponding methods-handlers
     * - Starts the subscriber
     */
    @OptIn(ExperimentalStdlibApi::class)
    fun <A : Aggregate> subscribe(subscriberInstance: Any) {
        val subscriberClass = subscriberInstance::class
        val subscriberInfo = AnnotationUtils.findAnnotation(subscriberClass.java, AggregateSubscriber::class.java)
            ?: throw IllegalStateException("No annotation ${AggregateSubscriber::class.simpleName} provided on class ${subscriberClass.simpleName}")

        val aggregateClass = try {
            subscriberInfo.aggregateClass as KClass<A>
        } catch (e: ClassCastException) {
            throw IllegalArgumentException(
                "Type parameter (aggregate type) doesn't match those provided " +
                        "in ${AggregateSubscriber::class.simpleName} annotation"
            )
        }

        val eventInfo = aggregateRegistry.getEventInfo(aggregateClass)
            ?: throw IllegalArgumentException("Couldn't find aggregate class ${aggregateClass.simpleName} in registry")

        val streamName = subscriberInfo.subscriberName.ifBlank {
            throw IllegalStateException("There is no name for subscriber provided in ${AggregateSubscriber::class.simpleName} annotation")
        }

        logger.info("Start creating subscription to aggregate: ${aggregateClass.simpleName} for ${subscriberClass.simpleName}")

        val subscriptionBuilder =
            eventsStreamManager.createEventStream(streamName, aggregateClass, subscriberInfo.retry)
                .toSubscriptionBuilder(eventMapper, eventInfo::getEventTypeByName)

        subscriberClass.memberFunctions.filter { // method has annotation filter
            it.findAnnotations(SubscribeEvent::class).size == 1
        }.filter {// method has only one arg and this is subtype of Event filter
            it.parameters.size == 2
                    && Event::class.isSuperclassOf(it.parameters[1].type.classifier as KClass<*>)
        }.filter { // event (method arg) is parametrised with correct aggregate type filter
            val eventType = it.parameters[1].type.classifier as KClass<Event<*>>
            eventType.supertypes.size == 1
                    && (eventType.supertypes[0].arguments[0].type?.classifier) == aggregateClass
        }.map {
            it to (it.parameters[1].type.classifier as KClass<Event<A>>)
        }.forEach { (method, event) ->
            subscriptionBuilder.`when`(event) {
                method.call(subscriberInstance, it)
            }
            logger.info(
                "Subscribing method ${subscriberClass.simpleName}#${method.name} " +
                        "to event ${event.simpleName} of aggregate ${aggregateClass.simpleName}"
            )
        }

        subscriptionBuilder.subscribe().also {
            subscribers.add(it)
        }
    }

    /**
     * Creates the subscriber of the aggregate event stream configuring it with an explicitly passed parameters.
     *
     * [handlersBlock] - lambda function. Library passes the newly created object of [EventHandlersRegistrar] as a
     * receiver of the lambda. This [EventHandlersRegistrar] contains methods that help you to define handlers for
     * certain types of events. So that you could use following syntax to create and initialize subscribers:
     *
     * ```
     * subscriptionsManager.createSubscriber(UserAggregate::class, "payment-service:user-view-subscriber") {
     *   `when`(UserCreatedEvent::class) { event ->
     *      logger.info("User created: {}", event.userName)
     *    }
     *   `when`(UserNameChanged`event::class) { event ->
     *      logger.info("User {} name changed from {} to {} ", event.userId, event.oldName, event.updatedName)
     *   }
     * }
     * ```
     */
    fun <A : Aggregate> createSubscriber(
        aggregateClass: KClass<A>,
        subscriberName: String,
        retryConf: RetryConf = RetryConf(3, RetryFailedStrategy.SKIP_EVENT),
        handlersBlock: EventHandlersRegistrar<A>.() -> Unit
    ): EventStreamSubscriber<A> {
        logger.info("Start creating subscription to aggregate: ${aggregateClass.simpleName}, subscriber name $subscriberName")

        val eventInfo = aggregateRegistry.getEventInfo(aggregateClass)
            ?: throw IllegalArgumentException("Couldn't find aggregate class ${aggregateClass.simpleName} in registry")

        val subscriptionBuilder =
            eventsStreamManager.createEventStream(subscriberName, aggregateClass, retryConf)
                .toSubscriptionBuilder(eventMapper, eventInfo::getEventTypeByName)

        handlersBlock.invoke(EventHandlersRegistrar(subscriptionBuilder)) // todo sukhoa maybe extension? .createRegistrar?

        return subscriptionBuilder.subscribe().also {
            subscribers.add(it)
        }
    }

    fun destroy() {
        subscribers.forEach {
            it.stopAndDestroy()
        }
    }

    /**
     * This class is used to pass as a receiver (like implicit this) to the trailing lambda parameter of the
     * [AggregateSubscriptionsManager.createSubscriber] method. It allows the caller code to invoke its
     * [EventHandlersRegistrar.when] method to register the handlers for the aggregate events.
     *
     * This class is supposed for only helping purposes. It abstracts away the process of creating the subscriber and
     * make syntax more concise.
     */
    class EventHandlersRegistrar<A : Aggregate>(
        private val subscriptionBuilder: EventStreamSubscriptionBuilder<A>
    ) {
        fun <E : Event<A>> `when`(
            eventType: KClass<E>,
            eventHandler: suspend (E) -> Unit
        ) {
            subscriptionBuilder.`when`(eventType, eventHandler)
        }
    }
}