package ru.quipy.projections

import org.slf4j.LoggerFactory
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import ru.quipy.saga.aggregate.api.*
import ru.quipy.streams.AggregateSubscriptionsManager
import javax.annotation.PostConstruct

@Component
class SagaProjections(
    private val sagaProjectionsRepository: SagaProjectionsRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager
) {
    private val logger = LoggerFactory.getLogger(SagaProjections::class.java)

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(SagaStepAggregate::class, "local-sagas::sagas-projections") {
            `when`(SagaStepLaunchedEvent::class) { event ->
                val sagaName = event.sagaName
                val stepId = event.sagaStepId.toString()
                val stepName = event.stepName

                val saga = Saga(event.sagaInstanceId.toString(), sagaName)

                saga.sagaSteps.add(SagaStep(stepName, stepId, event.prevSteps.toString(), event.createdAt.toString()))
                sagaProjectionsRepository.save(saga)

                logger.info("Started initiated Saga Event: $sagaName. Step: $stepName, id:$stepId")
            }

            `when`(SagaStepInitiatedEvent::class) { event ->
                val sagaName = event.sagaName
                val stepId = event.sagaStepId.toString()
                val stepName = event.stepName

                val saga = sagaProjectionsRepository.findById(event.sagaInstanceId.toString()).get()
                saga.sagaSteps.add(
                    SagaStep(
                        event.stepName,
                        event.sagaStepId.toString(),
                        event.prevSteps.toString(),
                        event.createdAt.toString()
                    )
                )
                sagaProjectionsRepository.save(saga)
                logger.info("Initiated Saga Event: $sagaName. Step: $stepName, id:$stepId")
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
    val prevStepsId: String?,
    val initiatedAt: String
)

@Repository
interface SagaProjectionsRepository : MongoRepository<Saga, String>
