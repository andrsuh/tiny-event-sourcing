package ru.quipy.saga.aggregate.stream

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.quipy.core.AggregateRegistry
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Aggregate
import ru.quipy.saga.SagaContext
import ru.quipy.saga.SagaInfo
import ru.quipy.saga.SagaStep
import ru.quipy.saga.aggregate.api.SagaStepAggregate
import ru.quipy.saga.aggregate.logic.SagaStepAggregateState
import ru.quipy.streams.*
import java.util.*
import java.util.concurrent.Executors

/**
 * Creates an event stream for each aggregate using [AggregateEventStreamManager].
 * These event streams read event records from DB, and if they contain Saga meta-information,
 * send commands to the sagaStepEsService to notify about the successful processing of the Saga step.
 */

class SagaEventStream(
    private val aggregateRegistry: AggregateRegistry,
    private val eventsStreamManager: AggregateEventStreamManager,
    private val sagaStepEsService: EventSourcingService<UUID, SagaStepAggregate, SagaStepAggregateState>
) {
    @Volatile
    private var active = true
    private val dispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher()
    private val logger = LoggerFactory.getLogger(SagaEventStream::class.java)

    fun init() {
        val aggregates = aggregateRegistry.getAllAggregates()
        //todo sukhoa
        aggregates.filter { it != SagaStepAggregate::class }
            .forEach {
                val streamName = "saga::" + it.simpleName
                val aggregateStream = eventsStreamManager.createEventStream(streamName, it)

                launchSagaEventStream(streamName, aggregateStream)
            }
    }

    private fun launchSagaEventStream(streamName: String, aggregateStream: AggregateEventStream<Aggregate>) {
        CoroutineScope(CoroutineName("reading-$streamName-coroutine") + dispatcher).launch {
            while (active) {
                try {
                    aggregateStream.handleNextRecord {
                        if (it.sagaContext != null)
                            processSagaSteps(it.sagaContext!!, it.eventTitle)
                        true
                    }
                } catch (e: Exception) {
                    logger.warn("Unexpected error in aggregate saga event stream ${streamName}.")
                }
            }
        }.also {
            //todo sukhoa handle
            it.invokeOnCompletion {}
        }
    }

    private fun processSagaSteps(sagaContext: SagaContext, eventName: String) {
        processMinSaga(sagaContext, eventName)

        sagaContext.ctx.forEach { context ->
            val sagaStep = contextToSagaStep(context)
            val sagaInstanceId = sagaStep.sagaInstanceId
            val sagaStepId = sagaStep.sagaStepId

            if (!sagaStepEsService.getState(sagaInstanceId)!!.containsProcessedSagaStep(sagaStepId)) {
                sagaStepEsService.update(sagaInstanceId) { it.processSagaStep(sagaStep, eventName) }
            }
        }
    }

    private fun processMinSaga(sagaContext: SagaContext, eventName: String) {
        if (sagaContext.causationId == null) {
            sagaStepEsService.create {
                it.processMinSaga(sagaContext.correlationId!!, sagaContext.currentEventId!!, eventName)
            }
        } else if (sagaStepEsService.getState(sagaContext.correlationId!!) != null) {
            sagaStepEsService.update(sagaContext.correlationId!!) {
                it.processMinSaga(
                    sagaContext.correlationId!!,
                    sagaContext.currentEventId!!,
                    eventName,
                    sagaContext.causationId
                )
            }
        }
    }

    private fun contextToSagaStep(ctx: Map.Entry<String, SagaInfo>): SagaStep {
        return SagaStep(
            sagaInstanceId = ctx.value.sagaInstanceId,
            sagaName = ctx.key,
            sagaStepId = ctx.value.sagaStepId,
            stepName = ctx.value.stepName,
            prevSteps = ctx.value.prevStepsIds
        )
    }
}