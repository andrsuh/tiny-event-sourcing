package ru.quipy.bankDemo.accounts.config

import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import java.util.UUID
class AccountBoundedContextConfig(val eventSourcingServiceFactory: EventSourcingServiceFactory) {
    fun accountEsService(): EventSourcingService<UUID, AccountAggregate, Account> =
        eventSourcingServiceFactory.create()
}