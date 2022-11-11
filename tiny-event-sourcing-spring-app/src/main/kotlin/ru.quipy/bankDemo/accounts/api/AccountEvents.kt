package ru.quipy.bankDemo.accounts.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.math.BigDecimal
import java.util.*

const val ACCOUNT_CREATED = "ACCOUNT_CREATED_EVENT"
const val BANK_ACCOUNT_CREATED = "BANK_ACCOUNT_CREATED_EVENT"
const val BANK_ACCOUNT_DEPOSIT = "BANK_ACCOUNT_DEPOSIT_EVENT"
const val BANK_ACCOUNT_WITHDRAWAL = "BANK_ACCOUNT_WITHDRAWAL_EVENT"
const val INTERNAL_ACCOUNT_TRANSFER = "INTERNAL_ACCOUNT_TRANSFER_EVENT"

const val TRANSFER_TRANSACTION_ACCEPTED = "TRANSFER_TRANSACTION_ACCEPTED"
const val TRANSFER_TRANSACTION_DECLINED = "TRANSFER_TRANSACTION_DECLINED"
const val TRANSFER_TRANSACTION_PROCESSED = "TRANSFER_TRANSACTION_PROCESSED"
const val TRANSFER_TRANSACTION_ROLLBACKED = "TRANSFER_TRANSACTION_ROLLBACKED"


@DomainEvent(name = ACCOUNT_CREATED)
data class AccountCreatedEvent(
    val accountId: UUID,
    val userId: UUID,
) : Event<AccountAggregate>(
    name = ACCOUNT_CREATED,
)

@DomainEvent(name = BANK_ACCOUNT_CREATED)
data class BankAccountCreatedEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
) : Event<AccountAggregate>(
    name = BANK_ACCOUNT_CREATED,
)

@DomainEvent(name = BANK_ACCOUNT_DEPOSIT)
data class BankAccountDepositEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
    val amount: BigDecimal,
) : Event<AccountAggregate>(
    name = BANK_ACCOUNT_DEPOSIT,
)

@DomainEvent(name = BANK_ACCOUNT_WITHDRAWAL)
data class BankAccountWithdrawalEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
    val amount: BigDecimal,
) : Event<AccountAggregate>(
    name = BANK_ACCOUNT_WITHDRAWAL,
)

@DomainEvent(name = INTERNAL_ACCOUNT_TRANSFER)
data class InternalAccountTransferEvent(
    val accountId: UUID,
    val bankAccountIdFrom: UUID,
    val bankAccountIdTo: UUID,
    val amount: BigDecimal,
) : Event<AccountAggregate>(
    name = INTERNAL_ACCOUNT_TRANSFER,
)

@DomainEvent(name = TRANSFER_TRANSACTION_ACCEPTED)
data class TransferTransactionAcceptedEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
    val transactionId: UUID,
    val transferAmount: BigDecimal,
    val isDeposit: Boolean
) : Event<AccountAggregate>(
    name = TRANSFER_TRANSACTION_ACCEPTED,
)

@DomainEvent(name = TRANSFER_TRANSACTION_DECLINED)
data class TransferTransactionDeclinedEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
    val transactionId: UUID,
    val reason: String
) : Event<AccountAggregate>(
    name = TRANSFER_TRANSACTION_DECLINED,
)

@DomainEvent(name = TRANSFER_TRANSACTION_PROCESSED)
data class TransferTransactionProcessedEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
    val transactionId: UUID,
) : Event<AccountAggregate>(
    name = TRANSFER_TRANSACTION_PROCESSED,
)

@DomainEvent(name = TRANSFER_TRANSACTION_ROLLBACKED)
data class TransferTransactionRollbackedEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
    val transactionId: UUID,
) : Event<AccountAggregate>(
    name = TRANSFER_TRANSACTION_ROLLBACKED,
)