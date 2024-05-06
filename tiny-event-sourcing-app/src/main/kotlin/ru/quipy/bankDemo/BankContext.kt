package ru.quipy.bankDemo

import com.mongodb.client.MongoDatabase
import ru.quipy.TinyEsLibConfig
import ru.quipy.application.component.Component
import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.accounts.config.AccountBoundedContextConfig
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.bankDemo.accounts.subscribers.TransactionsSubscriber
import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.config.TransactionCoundedContextConfig
import ru.quipy.bankDemo.transfers.db.BankAccountCacheRepository
import ru.quipy.bankDemo.transfers.db.BankAccountCacheRepositoryImpl
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.bankDemo.transfers.projections.BankAccountsExistenceCache
import ru.quipy.bankDemo.transfers.service.TransactionService
import ru.quipy.bankDemo.transfers.subscribers.BankAccountsSubscriber
import ru.quipy.core.EventSourcingService
import java.util.UUID

class BankContext private constructor() {
    lateinit var components: List<Component>
    lateinit var accountBoundedContextConfig: AccountBoundedContextConfig
    lateinit var transactionsSubscriber: TransactionsSubscriber
    lateinit var accountEventSourcingService: EventSourcingService<UUID, AccountAggregate, Account>

    lateinit var transactionCoundedContextConfig: TransactionCoundedContextConfig
    lateinit var bankAccountsExistenceCache: BankAccountsExistenceCache

    lateinit var bankAccountCacheRepository: BankAccountCacheRepository
    lateinit var transactionService : TransactionService
    lateinit var bankAccountsSubscriber: BankAccountsSubscriber
    lateinit var transactionEventSourcingService: EventSourcingService<UUID, TransferTransactionAggregate, TransferTransaction>
    constructor(tinyEsLibConfig: TinyEsLibConfig, mongoDatabase: MongoDatabase) : this() {
        accountBoundedContextConfig = AccountBoundedContextConfig(tinyEsLibConfig.eventSourcingServiceFactory)
        accountEventSourcingService = accountBoundedContextConfig.accountEsService()
        transactionsSubscriber = TransactionsSubscriber(tinyEsLibConfig.subscriptionsManager, accountEventSourcingService)

        transactionCoundedContextConfig = TransactionCoundedContextConfig(tinyEsLibConfig.eventSourcingServiceFactory)
        bankAccountCacheRepository = BankAccountCacheRepositoryImpl(mongoDatabase)

        bankAccountsExistenceCache = BankAccountsExistenceCache(bankAccountCacheRepository, tinyEsLibConfig.subscriptionsManager)

        transactionEventSourcingService = transactionCoundedContextConfig.transactionEsService()
        transactionService = TransactionService(bankAccountCacheRepository, transactionEventSourcingService)
        bankAccountsSubscriber = BankAccountsSubscriber(tinyEsLibConfig.subscriptionsManager, transactionEventSourcingService)

        components = mutableListOf(transactionsSubscriber, bankAccountsExistenceCache, transactionService, bankAccountsSubscriber)
        components.forEach { it.postConstruct() }
    }
}