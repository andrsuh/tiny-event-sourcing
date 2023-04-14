package ru.quipy.bankDemo.transfers.logic

import ru.quipy.bankDemo.transfers.api.*
import ru.quipy.bankDemo.transfers.logic.TransferTransaction.ParticipantState.*
import ru.quipy.bankDemo.transfers.logic.TransferTransaction.TransactionState.*
import ru.quipy.bankDemo.transfers.logic.TransferTransaction.TransactionState.NOT_CONFIRMED
import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.domain.Event
import java.math.BigDecimal
import java.util.*

class TransferTransaction : AggregateState<UUID, TransferTransactionAggregate> {
    private lateinit var transferId: UUID
    internal var transactionState = CREATED

    private lateinit var sourceParticipant: Participant
    private lateinit var destinationParticipant: Participant

    private lateinit var transferAmount: BigDecimal

    override fun getId() = transferId

    fun initiateTransferTransaction(
        id: UUID = UUID.randomUUID(),
        sourceAccountId: UUID,
        sourceBankAccountId: UUID,
        destinationAccountId: UUID,
        destinationBankAccountId: UUID,
        transferAmount: BigDecimal
    ): TransferTransactionCreatedEvent {
        // todo sukhoa validation
        return TransferTransactionCreatedEvent(
            id,
            sourceAccountId,
            sourceBankAccountId,
            destinationAccountId,
            destinationBankAccountId,
            transferAmount
        )
    }

    fun processParticipantAccept(bankAccountId: UUID): Event<TransferTransactionAggregate> {
        if (transactionState > HALF_CONFIRMED) return NoopEvent(transferId)

        when (bankAccountId) {
            sourceParticipant.bankAccountId -> {
                if (destinationParticipant.state != ACCEPTED) {
                    return TransferParticipantAcceptedEvent(transferId, sourceParticipant.bankAccountId)
                }
            }
            destinationParticipant.bankAccountId -> {
                if (sourceParticipant.state != ACCEPTED) {
                    return TransferParticipantAcceptedEvent(transferId, destinationParticipant.bankAccountId)
                }
            }
            else -> throw IllegalStateException("Transaction $transferId. No such participant bank account: $bankAccountId")
        }
        return TransactionConfirmedEvent(
            transferId,
            sourceParticipant.accountId,
            sourceParticipant.bankAccountId,
            destinationParticipant.accountId,
            destinationParticipant.bankAccountId
        )
    }

    fun processParticipantDecline(bankAccountId: UUID): Event<TransferTransactionAggregate> {
        if (transactionState > HALF_CONFIRMED) return NoopEvent(transferId)

        when (bankAccountId) {
            sourceParticipant.bankAccountId -> Unit
            destinationParticipant.bankAccountId -> Unit
            else -> throw IllegalStateException("Transaction $transferId. No such participant bank account: $bankAccountId")
        }

        return TransactionNotConfirmedEvent(
            transferId,
            sourceParticipant.accountId,
            sourceParticipant.bankAccountId,
            destinationParticipant.accountId,
            destinationParticipant.bankAccountId
        )
    }

    fun participantCommitted(bankAccountId: UUID): Event<TransferTransactionAggregate> {
        // todo sukhoa check that status is CONFIRMED
        when (bankAccountId) {
            sourceParticipant.bankAccountId -> {
                if (destinationParticipant.state != COMMITTED) {
                    return TransferParticipantCommittedEvent(transferId, bankAccountId)
                }
            }
            destinationParticipant.bankAccountId -> {
                if (sourceParticipant.state != COMMITTED) {
                    return TransferParticipantCommittedEvent(transferId, bankAccountId)
                }
            }
            else -> throw IllegalStateException("Transaction $transferId. No such participant bank account: $bankAccountId")
        }

        return TransactionSucceededEvent(transferId)
    }

    fun participantRollbacked(bankAccountId: UUID): Event<TransferTransactionAggregate> {
        // todo sukhoa check that status is NOT_CONFIRMED
        when (bankAccountId) {
            sourceParticipant.bankAccountId -> {
                if (destinationParticipant.state != ROLLBACKED) {
                    return TransferParticipantRollbackedEvent(transferId, bankAccountId)
                }
            }
            destinationParticipant.bankAccountId -> {
                if (sourceParticipant.state != ROLLBACKED) {
                    return TransferParticipantRollbackedEvent(transferId, bankAccountId)
                }
            }
            else -> {
                throw IllegalStateException("Transaction $transferId. No such participant bank account: $bankAccountId")
            }
        }

        return TransactionFailedEvent(transferId)
    }

    @StateTransitionFunc
    fun initiateTransferTransaction(event: TransferTransactionCreatedEvent) {
        this.transferId = event.transferId
        this.sourceParticipant = Participant(event.sourceAccountId, event.sourceBankAccountId)
        this.destinationParticipant = Participant(event.destinationAccountId, event.destinationBankAccountId)
        this.transferAmount = event.transferAmount
    }

    @StateTransitionFunc
    fun participantAccepted(event: TransferParticipantAcceptedEvent) {
        transactionState = HALF_CONFIRMED
        when (event.participantBankAccountId) {
            sourceParticipant.bankAccountId -> {
                sourceParticipant.acceptTransaction()
            }
            destinationParticipant.bankAccountId -> {
                destinationParticipant.acceptTransaction()
            }
        }
    }

    @StateTransitionFunc
    fun participantCommitted(event: TransferParticipantCommittedEvent) {
        transactionState = HALF_CONFIRMED
        when (event.participantBankAccountId) {
            sourceParticipant.bankAccountId -> {
                sourceParticipant.commitTransaction()
            }
            destinationParticipant.bankAccountId -> {
                destinationParticipant.commitTransaction()
            }
        }
    }

    @StateTransitionFunc
    fun participantCommitted(event: TransferParticipantRollbackedEvent) {
        transactionState = HALF_CONFIRMED
        when (event.participantBankAccountId) {
            sourceParticipant.bankAccountId -> {
                sourceParticipant.rollbackTransaction()
            }
            destinationParticipant.bankAccountId -> {
                destinationParticipant.rollbackTransaction()
            }
        }
    }

    @StateTransitionFunc
    fun participantDeclined(event: TransactionNotConfirmedEvent) {
        transactionState = NOT_CONFIRMED
    }

    @StateTransitionFunc
    fun confirmed(event: TransactionConfirmedEvent) {
        transactionState = CONFIRMED
        sourceParticipant.acceptTransaction()
        destinationParticipant.acceptTransaction()
    }

    @StateTransitionFunc
    fun succeeded(event: TransactionSucceededEvent) {
        transactionState = SUCCEEDED
        sourceParticipant.commitTransaction()
        destinationParticipant.commitTransaction()
    }

    @StateTransitionFunc
    fun failed(event: TransactionFailedEvent) {
        transactionState = FAILED
        sourceParticipant.rollbackTransaction()
        destinationParticipant.rollbackTransaction()
    }


    @StateTransitionFunc
    fun noop(event: NoopEvent) = Unit

    data class Participant(
        internal val accountId: UUID,
        internal val bankAccountId: UUID,
    ) {
        internal var state: ParticipantState = ParticipantState.NOT_CONFIRMED
            private set

        fun acceptTransaction() {
            state = ACCEPTED
        }

        fun commitTransaction() {
            state = COMMITTED
        }

        fun rollbackTransaction() {
            state = ROLLBACKED
        }
    }

    enum class ParticipantState {
        NOT_CONFIRMED,
        ACCEPTED,
        DECLINED,
        COMMITTED,
        ROLLBACKED
    }

    enum class TransactionState {
        CREATED,
        HALF_CONFIRMED,
        NOT_CONFIRMED,
        CONFIRMED,
        CANCELLED,
        SUCCEEDED,
        FAILED
    }
}