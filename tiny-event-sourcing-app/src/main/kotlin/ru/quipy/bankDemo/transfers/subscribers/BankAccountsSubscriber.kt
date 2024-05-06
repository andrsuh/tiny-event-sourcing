package ru.quipy.bankDemo.transfers.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.application.component.Component
import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.accounts.api.TransferTransactionAcceptedEvent
import ru.quipy.bankDemo.accounts.api.TransferTransactionDeclinedEvent
import ru.quipy.bankDemo.accounts.api.TransferTransactionProcessedEvent
import ru.quipy.bankDemo.accounts.api.TransferTransactionRollbackedEvent
import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.core.EventSourcingService
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.UUID

class BankAccountsSubscriber(
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val transactionEsService: EventSourcingService<UUID, TransferTransactionAggregate, TransferTransaction>
) : Component {
    private val logger: Logger = LoggerFactory.getLogger(BankAccountsSubscriber::class.java)
    override fun postConstruct() {
        subscriptionsManager.createSubscriber(AccountAggregate::class, "transactions::bank-accounts-subscriber") {
            `when`(TransferTransactionAcceptedEvent::class) { event ->
                transactionEsService.update(event.transactionId) {
                    it.processParticipantAccept(event.bankAccountId)
                }
            }
            `when`(TransferTransactionDeclinedEvent::class) { event ->
                transactionEsService.update(event.transactionId) {
                    it.processParticipantDecline(event.bankAccountId)
                }
            }
            `when`(TransferTransactionProcessedEvent::class) { event ->
                transactionEsService.update(event.transactionId) {
                    it.participantCommitted(event.bankAccountId)
                }
            }
            `when`(TransferTransactionRollbackedEvent::class) { event ->
                transactionEsService.update(event.transactionId) {
                    it.participantRollbacked(event.bankAccountId)
                }
            }
        }
    }
}