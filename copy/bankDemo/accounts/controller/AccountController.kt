package ru.quipy.bankDemo.accounts.controller

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.accounts.api.AccountCreatedEvent
import ru.quipy.bankDemo.accounts.api.BankAccountCreatedEvent
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.bankDemo.accounts.logic.BankAccount
import ru.quipy.core.EventSourcingService
import java.util.*

@RestController
@RequestMapping("/accounts")
class AccountController(
    val accountEsService: EventSourcingService<UUID, AccountAggregate, Account>
) {

    @PostMapping("/{holderId}")
    fun createAccount(@PathVariable holderId: UUID) : AccountCreatedEvent {
        return accountEsService.create { it.createNewAccount(holderId = holderId) }
    }

    @GetMapping("/{accountId}")
    fun getAccount(@PathVariable accountId: UUID) : Account? {
        return accountEsService.getState(accountId)
    }

    @PostMapping("/{accountId}/bankAccount")
    fun createBankAccount(@PathVariable accountId: UUID) : BankAccountCreatedEvent {
        return accountEsService.update(accountId) { it.createNewBankAccount() }
    }

    @GetMapping("/{accountId}/bankAccount/{bankAccountId}")
    fun getBankAccount(@PathVariable accountId: UUID, @PathVariable bankAccountId: UUID) : BankAccount? {
        return accountEsService.getState(accountId)?.bankAccounts?.get(bankAccountId)
    }
}