package ru.quipy

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.quipy.core.EventSourcingProperties
import java.math.BigDecimal
import java.util.UUID

class BankAggregateStateTest: BaseTest(testId.toString()) {
    companion object {
        private val testId = UUID.randomUUID()
        private val userId = UUID.randomUUID()
        @BeforeAll
        @JvmStatic
        fun configure() {
            configure(eventSourcingProperties = EventSourcingProperties())
        }
    }

    @BeforeEach
    fun init() {
        cleanDatabase()
    }

    @Test
    fun createAccount() {
        bankESService.create {
            it.createNewAccount(id = testId, holderId = userId)
        }

        val state = bankESService.getState(testId)!!

        Assertions.assertEquals(testId, state.getId())
    }

    @Test
    fun createBankAccount() {
        bankESService.create {
            it.createNewAccount(id = testId, holderId = userId)
        }
        val createdEvent = bankESService.update(testId) {
            it.createNewBankAccount()
        }

        val state = bankESService.getState(testId)!!

        Assertions.assertEquals(testId, state.getId())
        Assertions.assertEquals(1, state.bankAccounts.size)
        Assertions.assertNotNull(state.bankAccounts[createdEvent.bankAccountId])
        Assertions.assertEquals(createdEvent.bankAccountId, state.bankAccounts[createdEvent.bankAccountId]!!.id)
        Assertions.assertEquals(BigDecimal.ZERO, state.bankAccounts[createdEvent.bankAccountId]!!.balance)
    }

    @Test
    fun createBankAccountAndDeposit() {
        bankESService.create {
            it.createNewAccount(id = testId, holderId = userId)
        }

        val createdEvent = bankESService.update(testId) {
            it.createNewBankAccount()
        }

        val depositAmount = BigDecimal(100.0)
        // first deposit
        val depositEvent1 = bankESService.update(testId) {
            it.deposit(createdEvent.bankAccountId, depositAmount)
        }
        // second deposit
        val depositEvent2 = bankESService.update(testId) {
            it.deposit(createdEvent.bankAccountId, depositAmount)
        }

        val state = bankESService.getState(testId)!!

        Assertions.assertEquals(testId, state.getId())
        Assertions.assertEquals(1, state.bankAccounts.size)
        Assertions.assertNotNull(state.bankAccounts[createdEvent.bankAccountId])
        Assertions.assertEquals(createdEvent.bankAccountId, state.bankAccounts[createdEvent.bankAccountId]!!.id)
        Assertions.assertEquals(
            depositEvent1.amount + depositEvent2.amount,
            state.bankAccounts[createdEvent.bankAccountId]!!.balance
        )
    }

    @Test
    fun createTwoBankAccounts() {
        bankESService.create {
            it.createNewAccount(id = testId, holderId = userId)
        }

        val createdBankAccountEvent1 = bankESService.update(testId) {
            it.createNewBankAccount()
        }

        val createdBankAccountEvent2 = bankESService.update(testId) {
            it.createNewBankAccount()
        }

        val state = bankESService.getState(testId)!!

        Assertions.assertEquals(testId, state.getId())
        Assertions.assertEquals(2, state.bankAccounts.size)
        // first
        Assertions.assertNotNull(state.bankAccounts[createdBankAccountEvent1.bankAccountId])
        Assertions.assertEquals(
            state.bankAccounts[createdBankAccountEvent1.bankAccountId]!!.id,
            createdBankAccountEvent1.bankAccountId
        )
        Assertions.assertEquals(BigDecimal.ZERO, state.bankAccounts[createdBankAccountEvent1.bankAccountId]!!.balance)
        // second
        Assertions.assertNotNull(state.bankAccounts[createdBankAccountEvent2.bankAccountId])
        Assertions.assertEquals(
            createdBankAccountEvent2.bankAccountId,
            state.bankAccounts[createdBankAccountEvent2.bankAccountId]!!.id
        )
        Assertions.assertEquals(BigDecimal.ZERO, state.bankAccounts[createdBankAccountEvent2.bankAccountId]!!.balance)
    }

    @Test
    fun createTwoBankAccountsDepositAndTransfer() {
        bankESService.create {
            it.createNewAccount(id = testId, holderId = userId)
        }

        // first create and deposit
        val createdBankAccountEvent1 = bankESService.update(testId) {
            it.createNewBankAccount()
        }

        val depositAmount = BigDecimal(100.0)
        bankESService.update(testId) {
            it.deposit(createdBankAccountEvent1.bankAccountId, depositAmount)
        }

        // second create
        val createdBankAccountEvent2 = bankESService.update(testId) {
            it.createNewBankAccount()
        }

        // transfer
        val transferEvent = bankESService.update(testId) {
            it.transferBetweenInternalAccounts(
                createdBankAccountEvent1.bankAccountId,
                createdBankAccountEvent2.bankAccountId,
                depositAmount
            )
        }

        val state = bankESService.getState(testId)!!

        Assertions.assertEquals(2, state.bankAccounts.size)
        // first
        Assertions.assertNotNull(state.bankAccounts[transferEvent.bankAccountIdFrom])
        Assertions.assertNotNull(state.bankAccounts[transferEvent.bankAccountIdTo])

        Assertions.assertEquals(BigDecimal.ZERO, state.bankAccounts[transferEvent.bankAccountIdFrom]!!.balance)
        Assertions.assertEquals(transferEvent.amount, state.bankAccounts[transferEvent.bankAccountIdTo]!!.balance)
    }
}