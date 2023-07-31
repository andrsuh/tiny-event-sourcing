package ru.quipy.projections

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import ru.quipy.saga.aggregate.api.DefaultSagaProcessedEvent
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.streams.AggregateSubscriptionsManager
import javax.annotation.PostConstruct

@Component
class SagaDefaultProjections(
    private val sagaDefaultProjectionsRepository: SagaDefaultProjectionsRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager
) {
    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(SagaStepAggregate::class, "local-sagas::sagas-default-projections") {
            `when`(DefaultSagaProcessedEvent::class) { event ->
                val projectionOptional = sagaDefaultProjectionsRepository.findById(event.correlationId.toString())

                val currentEventId = event.currentEventId.toString()
                val causationId = event.causationId?.toString()

                val newStep = SagaDefaultStep(
                    currentEventId,
                    causationId,
                    event.createdAt.toString(),
                    event.eventName
                )

                val saga: SagaDefault
                if (projectionOptional.isEmpty) {
                    saga = SagaDefault(event.correlationId.toString())
                    insertNextStep(saga.sagaSteps, newStep)
                } else {
                    saga = projectionOptional.get()
                    if (!saga.sagaSteps.contains(newStep)) {
                        insertNextStep(saga.sagaSteps, newStep)
                    }
                }

                sagaDefaultProjectionsRepository.save(saga)
            }
        }
    }

    private fun insertNextStep(sagaSteps: MutableList<SagaDefaultStep>, sagaStep: SagaDefaultStep) {
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

@Document("sagas-default-projections")
data class SagaDefault(
    @Id
    val correlationId: String,
    val sagaSteps: MutableList<SagaDefaultStep> = mutableListOf()
)

data class SagaDefaultStep(
    val currentEventId: String,
    val causationId: String? = null,
    val processedAt: String,
    var eventName: String
)

@Repository
interface SagaDefaultProjectionsRepository : MongoRepository<SagaDefault, String>