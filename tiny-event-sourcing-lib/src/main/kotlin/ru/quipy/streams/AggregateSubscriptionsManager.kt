package ru.quipy.streams

import org.slf4j.LoggerFactory
import org.springframework.core.annotation.AnnotationUtils
import ru.quipy.domain.Aggregate
import ru.quipy.domain.Event
import ru.quipy.streams.EventStreamSubscriber.EventStreamSubscriptionBuilder
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotations
import kotlin.reflect.full.isSuperclassOf
import kotlin.reflect.full.memberFunctions

class AggregateSubscriptionsManager(
    private val eventsStreamManager: AggregateEventsStreamManager
) {
    private val logger = LoggerFactory.getLogger(AggregateSubscriptionsManager::class.java)

    private val subscribers: MutableList<EventStreamSubscriber<*>> = mutableListOf()

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

        val streamName = subscriberInfo.subscriberName.ifBlank {
            throw IllegalStateException("There is no name for subscriber provided in ${AggregateSubscriber::class.simpleName} annotation")
        }

        logger.info("Start creating subscription to aggregate: ${aggregateClass.simpleName} for ${subscriberClass.simpleName}")

        val subscriptionBuilder =
            eventsStreamManager.createEventStream(streamName, aggregateClass).toSubscriptionBuilder()

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

    fun <A : Aggregate> createSubscriber(
        aggregateClass: KClass<A>,
        subscriberName: String,
        handlersBlock: EventHandlersRegistrar<A>.() -> Unit
    ): EventStreamSubscriber<A> {
        logger.info("Start creating subscription to aggregate: ${aggregateClass.simpleName}, subscriber name $subscriberName")

        val subscriptionBuilder =
            eventsStreamManager.createEventStream(subscriberName, aggregateClass).toSubscriptionBuilder()

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

    class EventHandlersRegistrar<A : Aggregate>(
        private val subscriptionBuilder: EventStreamSubscriptionBuilder<A>
    ) {
        /**
         * todo sukhoa docs!
         */
        fun <E : Event<A>> `when`(
            eventType: KClass<E>,
            eventHandler: suspend (E) -> Unit
        ) {
            subscriptionBuilder.`when`(eventType, eventHandler)
        }
    }
}