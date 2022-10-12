package ru.quipy.bankDemo.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.math.BigDecimal
import java.util.*

const val ACCOUNT_CREATED = "ACCOUNT_CREATED_EVENT"
const val BANK_ACCOUNT_CREATED = "BANK_ACCOUNT_CREATED_EVENT"
const val BANK_ACCOUNT_DEPOSIT = "BANK_ACCOUNT_DEPOSIT_EVENT"
const val BANK_ACCOUNT_WITHDRAWAL = "BANK_ACCOUNT_WITHDRAWAL_EVENT"
const val INTERNAL_ACCOUNT_TRANSFER = "INTERNAL_ACCOUNT_TRANSFER_EVENT"


@DomainEvent(name = ACCOUNT_CREATED)
class AccountCreatedEvent(
    val accountId: UUID,
) : Event<AccountAggregate>(
    name = ACCOUNT_CREATED,
    createdAt = System.currentTimeMillis(),
)

@DomainEvent(name = BANK_ACCOUNT_CREATED)
class BankAccountCreatedEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
) : Event<AccountAggregate>(
    name = BANK_ACCOUNT_CREATED,
    createdAt = System.currentTimeMillis(),
)

@DomainEvent(name = BANK_ACCOUNT_DEPOSIT)
class BankAccountDepositEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
    val amount: BigDecimal,
) : Event<AccountAggregate>(
    name = BANK_ACCOUNT_DEPOSIT,
    createdAt = System.currentTimeMillis(),
)

@DomainEvent(name = BANK_ACCOUNT_WITHDRAWAL)
class BankAccountWithdrawalEvent(
    val accountId: UUID,
    val bankAccountId: UUID,
    val amount: BigDecimal,
) : Event<AccountAggregate>(
    name = BANK_ACCOUNT_WITHDRAWAL,
    createdAt = System.currentTimeMillis(),
)

@DomainEvent(name = INTERNAL_ACCOUNT_TRANSFER)
class InternalAccountTransferEvent(
    val accountId: UUID,
    val bankAccountIdFrom: UUID,
    val bankAccountIdTo: UUID,
    val amount: BigDecimal,
) : Event<AccountAggregate>(
    name = INTERNAL_ACCOUNT_TRANSFER,
    createdAt = System.currentTimeMillis(),
)