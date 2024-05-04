package ru.quipy.bankDemo.transfers.config

import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import java.util.UUID

class TransactionCoundedContextConfig(val eventSourcingServiceFactory: EventSourcingServiceFactory) {

    fun transactionEsService(): EventSourcingService<UUID, TransferTransactionAggregate, TransferTransaction> =
        eventSourcingServiceFactory.create()

}