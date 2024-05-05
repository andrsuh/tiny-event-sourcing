package ru.quipy.bankDemo.transfers.db

import ru.quipy.bankDemo.transfers.db.entity.BankAccount
import java.util.Optional
import java.util.UUID

interface BankAccountCacheRepository {
    fun save(bankAccount: BankAccount)
    fun findById(id: UUID) : Optional<BankAccount>
    fun existsById(bankAccountId: UUID) : Boolean
}