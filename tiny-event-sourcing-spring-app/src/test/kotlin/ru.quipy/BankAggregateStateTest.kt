package ru.quipy

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.core.EventSourcingService
import java.math.BigDecimal
import java.util.*

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class BankAggregateStateTest {
    companion object {
        private val testId = UUID.randomUUID()
        private val userId = UUID.randomUUID()
    }

    @Autowired
    private lateinit var bankESService: EventSourcingService<UUID, AccountAggregate, Account>

    @Autowired
    lateinit var mongoTemplate: MongoTemplate

    @BeforeEach
    fun init() {
        cleanDatabase()
    }

    fun cleanDatabase() {
        mongoTemplate.remove(Query.query(Criteria.where("aggregateId").`is`(testId)), "accounts")
        mongoTemplate.remove(Query.query(Criteria.where("_id").`is`(testId)), "snapshots")
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