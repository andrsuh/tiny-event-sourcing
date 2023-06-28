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
                val causationId = event.causationId?.toString()

                val newStep = SagaMinStep(
                    currentEventId,
                    causationId,
                    event.createdAt.toString(),
                    event.eventName
                )

                val saga: SagaMin
                if (projectionOptional.isEmpty) {
                    saga = SagaMin(event.correlationId.toString())
                    insertNextStep(saga.sagaSteps, newStep)
                } else {
                    saga = projectionOptional.get()
                    if (!saga.sagaSteps.contains(newStep)) {
                        insertNextStep(saga.sagaSteps, newStep)
                    }
                }

                sagaMinProjectionsRepository.save(saga)
            }
        }
    }

    private fun insertNextStep(sagaSteps: MutableList<SagaMinStep>, sagaStep: SagaMinStep) {
        var indexToInsert = 0
        if (sagaStep.causationId != null) {
            indexToInsert = sagaSteps.indices.findLast {
                sagaStep.causationId == sagaSteps[it].currentEventId
            }?.inc()
                ?: sagaSteps.size
        }

        if (indexToInsert < sagaSteps.size) {
            sagaSteps.add(indexToInsert, sagaStep)
        } else {
            sagaSteps.add(sagaStep)
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