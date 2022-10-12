package ru.quipy.bankDemo.logic

import ru.quipy.bankDemo.api.*
import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import java.math.BigDecimal
import java.util.*

// todo написать в доку, что нужен пустой конструктор...
// todo стоит написать, что в стейте не может быть закрытых полей, потому как нихера не сериализуется. Ну или определенные настройки маппера нужны
// вопрос, что делать, если, скажем, обрабатываем какой-то ивент, понимаем, что агрегата, который нужно обновить не существует.
// Может ли ивент (ошибка) существовать в отрыве от агрегата?
class Account : AggregateState<UUID, AccountAggregate> {
    override lateinit var aggregateId: UUID
    var bankAccounts: MutableMap<UUID, BankAccount> = mutableMapOf()

    fun createNewAccount(id: UUID): AccountCreatedEvent {
        return AccountCreatedEvent(id)
    }

    fun createNewBankAccount(): BankAccountCreatedEvent {
        if (bankAccounts.size >= 5)
            throw IllegalStateException("Account $aggregateId already has ${bankAccounts.size} bank accounts")

        return BankAccountCreatedEvent(accountId = aggregateId, bankAccountId = UUID.randomUUID())
    }

    fun deposit(toBankAccountId: UUID, amount: BigDecimal): BankAccountDepositEvent {
        bankAccounts[toBankAccountId]
            ?: throw IllegalArgumentException("No such account to transfer to: $toBankAccountId")

        if (bankAccounts.values.sumOf { it.amount } + amount > BigDecimal(15_000_000))
            throw IllegalStateException("You can't store more than 15.000.000")


        return BankAccountDepositEvent(
            accountId = aggregateId,
            bankAccountId = toBankAccountId,
            amount = amount
        )
    }

    fun withdraw(fromBankAccountId: UUID, amount: BigDecimal): BankAccountWithdrawalEvent {
        val fromBankAccount = bankAccounts[fromBankAccountId]
            ?: throw IllegalArgumentException("No such account to withdraw from: $fromBankAccountId")

        if (amount > fromBankAccount.amount) {
            throw IllegalArgumentException("Cannot withdraw $amount. Not enough money: ${fromBankAccount.amount}")
        }

        return BankAccountWithdrawalEvent(
            accountId = aggregateId,
            bankAccountId = fromBankAccountId,
            amount = amount
        )
    }

    fun transferBetweenInternalAccounts(
        fromBankAccountId: UUID,
        toBankAccountId: UUID,
        transferAmount: BigDecimal
    ): InternalAccountTransferEvent {
        val bankAccountFrom = bankAccounts[fromBankAccountId]
            ?: throw IllegalArgumentException("No such account to withdraw from: $fromBankAccountId")

        if (transferAmount > bankAccountFrom.amount) {
            throw IllegalArgumentException("Cannot withdraw $transferAmount. Not enough money: ${bankAccountFrom.amount}")
        }

        bankAccounts[toBankAccountId]
            ?: throw IllegalArgumentException("No such account to transfer to: $toBankAccountId")

        return InternalAccountTransferEvent(
            accountId = aggregateId,
            bankAccountIdFrom = fromBankAccountId,
            bankAccountIdTo = toBankAccountId,
            amount = transferAmount
        )
    }

    @StateTransitionFunc
    fun createNewBankAccount(event: BankAccountCreatedEvent) {
        bankAccounts[event.bankAccountId] = BankAccount(event.bankAccountId)
    }

    @StateTransitionFunc
    fun createNewBankAccount(event: AccountCreatedEvent) {
        aggregateId = event.accountId
    }

    @StateTransitionFunc
    fun deposit(event: BankAccountDepositEvent) {
        bankAccounts[event.bankAccountId]!!.deposit(event.amount)
    }

    @StateTransitionFunc
    fun withdraw(event: BankAccountWithdrawalEvent) {
        bankAccounts[event.bankAccountId]!!.withdraw(event.amount)
    }

    @StateTransitionFunc
    fun internalAccountTransfer(event: InternalAccountTransferEvent) {
        bankAccounts[event.bankAccountIdFrom]!!.withdraw(event.amount)
        bankAccounts[event.bankAccountIdTo]!!.deposit(event.amount)
    }
}


data class BankAccount(
    val id: UUID,
    internal var amount: BigDecimal = BigDecimal.ZERO,
) {
    fun deposit(amount: BigDecimal) {
        this.amount = this.amount.add(amount)
    }

    fun withdraw(amount: BigDecimal) {
        this.amount = this.amount.subtract(amount)
    }
}