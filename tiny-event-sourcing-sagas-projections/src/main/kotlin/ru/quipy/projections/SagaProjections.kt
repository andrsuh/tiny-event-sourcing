package ru.quipy.projections

import org.slf4j.LoggerFactory
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import ru.quipy.saga.aggregate.api.*
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
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

                val newStep = SagaStep(
                    stepName,
                    stepId,
                    event.prevSteps.map { it.toString() }.toSet(),
                    event.createdAt.toString()
                )
                insertNextStep(saga.sagaSteps, newStep)

                sagaProjectionsRepository.save(saga)
                logger.info("Started initiated Saga Event: $sagaName. Step: $stepName, id:$stepId")
            }

            `when`(SagaStepInitiatedEvent::class) { event ->
                val sagaName = event.sagaName
                val stepId = event.sagaStepId.toString()
                val stepName = event.stepName

                val saga = sagaProjectionsRepository.findById(event.sagaInstanceId.toString()).get()

                val newStep = SagaStep(
                    stepName,
                    stepId,
                    event.prevSteps.map { it.toString() }.toSet(),
                    event.createdAt.toString()
                )
                insertNextStep(saga.sagaSteps, newStep)

                sagaProjectionsRepository.save(saga)
                logger.info("Initiated Saga Event: $sagaName. Step: $stepName, id:$stepId")
            }

            `when`(SagaStepProcessedEvent::class) { event ->
                val saga = sagaProjectionsRepository.findById(event.sagaInstanceId.toString()).get()

                val sagaName = event.sagaName
                val stepId = event.sagaStepId.toString()
                val stepName = event.stepName

                val sagaStep = saga.sagaSteps.find { it.sagaStepId == stepId }
                sagaStep?.processedAt = event.createdAt.toString()
                sagaStep?.eventName = event.eventName

                sagaProjectionsRepository.save(saga)
                logger.info("Processed Saga Event: $sagaName. Step: $stepName, id:$stepId")
            }
        }
    }

    private fun insertNextStep(sagaSteps: MutableList<SagaStep>, sagaStep: SagaStep) {
        var indexToInsert = 0

        if (sagaStep.prevStepsId.isNotEmpty()) {
            indexToInsert = sagaSteps.indices.findLast {
                sagaStep.prevStepsId.contains(sagaSteps[it].sagaStepId)
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

@Document("sagas-projections")
data class Saga(
    @Id
    val sagaInstanceId: String,
    val sagaName: String?,
    val sagaSteps: MutableList<SagaStep> = mutableListOf()
)

data class SagaStep(
    val stepName: String,
    val sagaStepId: String,
    val prevStepsId: Set<String> = setOf(),
    val initiatedAt: String,
    var processedAt: String? = null,
    var eventName: String? = null
)

@Repository
interface SagaProjectionsRepository : MongoRepository<Saga, String>
