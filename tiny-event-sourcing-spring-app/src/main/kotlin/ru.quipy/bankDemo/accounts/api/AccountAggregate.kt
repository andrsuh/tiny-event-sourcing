package ru.quipy.bankDemo.accounts.api

import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate

@AggregateType(aggregateEventsTableName = "accounts")
class AccountAggregate: Aggregate