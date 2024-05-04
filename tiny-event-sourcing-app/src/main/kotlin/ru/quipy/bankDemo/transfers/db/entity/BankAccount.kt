package ru.quipy.bankDemo.transfers.db.entity

import java.util.UUID

data class BankAccount(
    val bankAccountId: UUID,
    var accountId: UUID
)