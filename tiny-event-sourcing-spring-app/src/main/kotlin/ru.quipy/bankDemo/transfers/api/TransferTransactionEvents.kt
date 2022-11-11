package ru.quipy.bankDemo.transfers.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.math.BigDecimal
import java.util.*

const val TRANSFER_TRANSACTION_CREATED = "TRANSFER_TRANSACTION_CREATED"
const val TRANSFER_PARTICIPANT_ACCEPTED = "TRANSFER_PARTICIPANT_ACCEPTED"
const val TRANSFER_CONFIRMED = "TRANSFER_CONFIRMED"
const val TRANSFER_NOT_CONFIRMED = "TRANSFER_NOT_CONFIRMED"
const val TRANSFER_PARTICIPANT_COMMITTED = "TRANSFER_PARTICIPANT_COMMITTED"
const val TRANSFER_SUCCEEDED = "TRANSFER_SUCCEEDED"
const val TRANSFER_PARTICIPANT_ROLLBACKED = "TRANSFER_PARTICIPANT_ROLLBACKED"
const val TRANSFER_FAILED = "TRANSFER_FAILED"
const val NOOP = "NOOP"
const val TRANSFER_CANCELLED = "TRANSFER_CANCELLED"

@DomainEvent(name = TRANSFER_TRANSACTION_CREATED)
data class TransferTransactionCreatedEvent(
    val transferId: UUID,
    val sourceAccountId: UUID,
    val sourceBankAccountId: UUID,
    val destinationAccountId: UUID,
    val destinationBankAccountId: UUID,
    val transferAmount: BigDecimal,
) : Event<TransferTransactionAggregate>(
    name = TRANSFER_TRANSACTION_CREATED,
)

@DomainEvent(name = TRANSFER_PARTICIPANT_ACCEPTED)
data class TransferParticipantAcceptedEvent(
    val transferId: UUID,
    val participantBankAccountId: UUID,
) : Event<TransferTransactionAggregate>(
    name = TRANSFER_PARTICIPANT_ACCEPTED,
)

@DomainEvent(name = TRANSFER_CONFIRMED)
data class TransactionConfirmedEvent(
    val transferId: UUID,
    val sourceAccountId: UUID,
    val sourceBankAccountId: UUID,
    val destinationAccountId: UUID,
    val destinationBankAccountId: UUID,
) : Event<TransferTransactionAggregate>(
    name = TRANSFER_CONFIRMED,
)

@DomainEvent(name = TRANSFER_NOT_CONFIRMED)
data class TransactionNotConfirmedEvent(
    val transferId: UUID,
    val sourceAccountId: UUID,
    val sourceBankAccountId: UUID,
    val destinationAccountId: UUID,
    val destinationBankAccountId: UUID,
) : Event<TransferTransactionAggregate>(
    name = TRANSFER_NOT_CONFIRMED,
)

@DomainEvent(name = NOOP)
data class NoopEvent(
    val transferId: UUID,
) : Event<TransferTransactionAggregate>(
    name = NOOP,
)

@DomainEvent(name = TRANSFER_PARTICIPANT_COMMITTED)
data class TransferParticipantCommittedEvent(
    val transferId: UUID,
    val participantBankAccountId: UUID,
) : Event<TransferTransactionAggregate>(
    name = TRANSFER_PARTICIPANT_COMMITTED,
)

@DomainEvent(name = TRANSFER_PARTICIPANT_ROLLBACKED)
data class TransferParticipantRollbackedEvent(
    val transferId: UUID,
    val participantBankAccountId: UUID,
) : Event<TransferTransactionAggregate>(
    name = TRANSFER_PARTICIPANT_ROLLBACKED,
)


@DomainEvent(name = TRANSFER_SUCCEEDED)
data class TransactionSucceededEvent(
    val transferId: UUID,
) : Event<TransferTransactionAggregate>(
    name = TRANSFER_SUCCEEDED,
)

@DomainEvent(name = TRANSFER_FAILED)
data class TransactionFailedEvent(
    val transferId: UUID,
) : Event<TransferTransactionAggregate>(
    name = TRANSFER_FAILED,
)

//@DomainEvent(name = TRANSFER_CANCELLED)
//data class TransactionCancelledEvent(
//    val transferId: UUID,
//    val sourceAccountId: UUID,
//    val sourceBankAccountId: UUID,
//    val destinationAccountId: UUID,
//    val destinationBankAccountId: UUID,
//) : Event<TransferTransactionAggregate>(
//    name = TRANSFER_CANCELLED,
//    createdAt = System.currentTimeMillis(),
//)
