package ru.quipy

import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.core.EventSourcingProperties
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID

class TransferTransactionAggregateStateTest: BaseTest(testId) {
    companion object {
        private val testAccountId = UUID.fromString("b88f83bf-9a2a-4091-9cb3-3185f6f65a4b")
        private val testAccount2Id = UUID.fromString("1fccc03e-4ed3-47b7-8f76-8e62efb5e36e")
        private val userId = UUID.fromString("330f9c97-4031-4bd4-ab49-a347719ace25")
        private const val testId = "TransferTransactionAggregateStateTest"

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

    override fun cleanDatabase() {
        super.cleanDatabase()
        mongoDatabase.getCollection("accounts").drop()
        mongoDatabase.getCollection("snapshots").drop()
        mongoDatabase.getCollection("transfers").drop()
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