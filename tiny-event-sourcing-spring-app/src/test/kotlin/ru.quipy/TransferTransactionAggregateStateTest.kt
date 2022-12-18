package ru.quipy

import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.bankDemo.transfers.projections.BankAccountCacheRepository
import ru.quipy.bankDemo.transfers.service.TransactionService
import ru.quipy.core.EventSourcingService
import java.math.BigDecimal
import java.time.Duration
import java.util.*

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TransferTransactionAggregateStateTest {
    companion object {
        private val testAccountId = UUID.fromString("b88f83bf-9a2a-4091-9cb3-3185f6f65a4b")
        private val testAccount2Id = UUID.fromString("1fccc03e-4ed3-47b7-8f76-8e62efb5e36e")
        private val userId = UUID.fromString("330f9c97-4031-4bd4-ab49-a347719ace25")
    }

    @Autowired
    private lateinit var accountEsService: EventSourcingService<UUID, AccountAggregate, Account>

    @Autowired
    private lateinit var transactionEsService: EventSourcingService<UUID, TransferTransactionAggregate, TransferTransaction>

    @Autowired
    private lateinit var transactionService: TransactionService

    @Autowired
    private lateinit var bankAccountCacheRepository: BankAccountCacheRepository

    @Autowired
    lateinit var mongoTemplate: MongoTemplate

    @BeforeEach
    fun init() {
        cleanDatabase()
    }

    fun cleanDatabase() {
        mongoTemplate.remove(Query.query(Criteria.where("aggregateId").`is`(testAccountId)), "accounts")
        mongoTemplate.remove(Query.query(Criteria.where("aggregateId").`is`(testAccount2Id)), "accounts")
        mongoTemplate.remove(Query.query(Criteria.where("_id").`is`(testAccountId)), "snapshots")
        mongoTemplate.remove(Query.query(Criteria.where("_id").`is`(testAccount2Id)), "snapshots")
        mongoTemplate.remove(Query(), "transfers")
    }

    @Test
    fun createTwoBankAccountsDepositAndTransfer() {
        val account1 = accountEsService.create {
            it.createNewAccount(id = testAccountId, holderId = userId)
        }

        // first create and deposit
        val createdBankAccountEvent1 = accountEsService.update(testAccountId) {
            it.createNewBankAccount()
        }

        val depositAmount = BigDecimal(100.0)
        accountEsService.update(testAccountId) {
            it.deposit(createdBankAccountEvent1.bankAccountId, depositAmount)
        }

        val account2 = accountEsService.create {
            it.createNewAccount(id = testAccount2Id, holderId = userId)
        }

        // second create
        val createdBankAccountEvent2 = accountEsService.update(testAccount2Id) {
            it.createNewBankAccount()
        }

        Awaitility.await().atMost(Duration.ofSeconds(10)).until {
            bankAccountCacheRepository.existsById(createdBankAccountEvent2.bankAccountId)
        }

        // transfer
        val transferEvent = transactionService.initiateTransferTransaction(
            createdBankAccountEvent1.bankAccountId,
            createdBankAccountEvent2.bankAccountId,
            BigDecimal(100.0)
        )

        Awaitility.await().atMost(Duration.ofSeconds(10)).until {
            val transaction = transactionEsService.getState(transferEvent.transferId)!!

            transaction.transactionState == TransferTransaction.TransactionState.SUCCEEDED
        }

        val state1 = accountEsService.getState(testAccountId)!!
        val state2 = accountEsService.getState(testAccount2Id)!!

        state1.bankAccounts[transferEvent.sourceBankAccountId]!!.balance == BigDecimal.ZERO &&
                state2.bankAccounts[transferEvent.destinationBankAccountId]!!.balance == transferEvent.transferAmount

        Assertions.assertTrue(existsAccountState(testAccountId) { account, version ->
            val bankAccount = account.bankAccounts[transferEvent.sourceBankAccountId]
                ?: return@existsAccountState false

            val pendingTransaction = bankAccount.pendingTransactions[transferEvent.transferId]
                ?: return@existsAccountState false

            return@existsAccountState bankAccount.balance == (depositAmount.subtract(pendingTransaction.transferAmountFrozen))
        })
    }

    private fun existsAccountState(aggregateId: UUID, predicate: (Account, Long) -> Boolean): Boolean {
        var version = 1L
        var state = accountEsService.getStateOfVersion(aggregateId, version)
        while (state != null) {
            if (predicate.invoke(state, version)) return true
            version++
            state = accountEsService.getStateOfVersion(aggregateId, version)
        }
        return false
    }
}