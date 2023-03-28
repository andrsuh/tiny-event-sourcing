package ru.quipy.projections

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.saga.aggregate.api.SagaStepProcessedEvent
import ru.quipy.saga.aggregate.api.SagaStepStartedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import javax.annotation.PostConstruct

@Component
class SagaProjections(
    private val sagaProjectionsRepository: SagaProjectionsRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager
) {

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(SagaStepAggregate::class, "local-sagas-launch::sagas-projections") {
            `when`(SagaStepStartedEvent::class) { event ->
                val saga = Saga(event.sagaInstanceId.toString(), event.sagaName)
                saga.sagaSteps.add(SagaStep(event.stepName, event.sagaStepId.toString(), event.prevStep.toString()))
                sagaProjectionsRepository.save(saga)
            }
        }

        subscriptionsManager.createSubscriber(SagaStepAggregate::class, "local-sagas-process::sagas-projections") {
            `when`(SagaStepProcessedEvent::class) { event ->
                val saga = sagaProjectionsRepository.findById(event.sagaInstanceId.toString()).get()
                saga.sagaSteps.add(SagaStep(event.stepName, event.sagaStepId.toString(), event.prevStep.toString()))
                sagaProjectionsRepository.save(saga)
            }
        }
    }
}

@Document("sagas-projections")
data class Saga(
    @Id
    val sagaInstanceId: String,
    val sagaName: String,
    val sagaSteps: MutableList<SagaStep> = mutableListOf()
)

data class SagaStep(
    val stepName: String,
    val sagaStepId: String,
    val prevStepId: String?
)

@Repository
interface SagaProjectionsRepository : MongoRepository<Saga, String>
