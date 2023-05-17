package ru.quipy.projections

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import ru.quipy.saga.aggregate.api.MinSagaProcessedEvent
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.streams.AggregateSubscriptionsManager
import javax.annotation.PostConstruct

@Component
class SagaMinProjections(
    private val sagaMinProjectionsRepository: SagaMinProjectionsRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager
) {
    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(SagaStepAggregate::class, "local-sagas::sagas-min-projections") {
            `when`(MinSagaProcessedEvent::class) { event ->
                val projectionOptional = sagaMinProjectionsRepository.findById(event.correlationId.toString())

                val currentEventId = event.currentEventId.toString()
                val causationId = event.causationId.toString()

                val sagaStep = SagaMinStep(
                    currentEventId,
                    causationId,
                    event.createdAt.toString(),
                    event.eventName
                )

                if (projectionOptional.isEmpty) {
                    val saga = SagaMin(event.correlationId.toString())
                    saga.sagaSteps.add(sagaStep)
                    sagaMinProjectionsRepository.save(saga)

                } else {
                    val saga = projectionOptional.get()
                    if (!saga.sagaSteps.contains(sagaStep)) {
                        saga.sagaSteps.add(sagaStep)
                    }

                    sagaMinProjectionsRepository.save(saga)
                }
            }
        }
    }
}

@Document("sagas-min-projections")
data class SagaMin(
    @Id
    val correlationId: String,
    val sagaSteps: MutableList<SagaMinStep> = mutableListOf()
)

data class SagaMinStep(
    val currentEventId: String,
    val causationId: String? = null,
    val processedAt: String,
    var eventName: String
)

@Repository
interface SagaMinProjectionsRepository : MongoRepository<SagaMin, String>