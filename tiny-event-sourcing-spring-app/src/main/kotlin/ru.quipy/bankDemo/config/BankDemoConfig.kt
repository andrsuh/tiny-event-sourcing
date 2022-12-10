package ru.quipy.bankDemo.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.bankDemo.api.AccountAggregate
import ru.quipy.bankDemo.logic.Account
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import java.util.*

@Configuration
class BankDemoConfig {

    @Bean
    fun bankESService(
        eventSourcingServiceFactory: EventSourcingServiceFactory
    ) : EventSourcingService<UUID, AccountAggregate, Account> = eventSourcingServiceFactory.create()
}