package ru.quipy.bankDemo.transfers.projections

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.application.component.Component
import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.accounts.api.BankAccountCreatedEvent
import ru.quipy.bankDemo.transfers.db.BankAccountCacheRepository
import ru.quipy.bankDemo.transfers.db.entity.BankAccount
import ru.quipy.streams.AggregateSubscriptionsManager

class BankAccountsExistenceCache(
    private val bankAccountCacheRepository: BankAccountCacheRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager
) : Component {
    private val logger: Logger = LoggerFactory.getLogger(BankAccountsExistenceCache::class.java)

    override fun postConstruct() {
        subscriptionsManager.createSubscriber(AccountAggregate::class, "transactions::accounts-cache") {
            `when`(BankAccountCreatedEvent::class) { event ->
                bankAccountCacheRepository.save(BankAccount(event.bankAccountId, event.accountId)) // todo sukhoa idempotence!
                logger.info("Update bank account cache, create account ${event.accountId}-${event.bankAccountId}")
            }
            // todo sukhoa bank account deleted event
        }
    }
}
