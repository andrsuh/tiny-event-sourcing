package ru.quipy.bankDemo.transfers.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import ru.quipy.bankDemo.accounts.api.*
import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.api.TransferTransactionCreatedEvent
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.core.EventSourcingService
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Component
class BankAccountsSubscriber(
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val transactionEsService: EventSourcingService<UUID, TransferTransactionAggregate, TransferTransaction>
) {
    private val logger: Logger = LoggerFactory.getLogger(BankAccountsSubscriber::class.java)

    @PostConstruct
    fun init() {
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