package ru.quipy.bankDemo.transfers.db

import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.eq
import org.bson.Document
import ru.quipy.bankDemo.transfers.db.entity.BankAccount
import java.util.Optional
import java.util.UUID

class BankAccountCacheRepositoryImpl(private val mongoDatabase: MongoDatabase): BankAccountCacheRepository {
    override fun save(bankAccount: BankAccount) {
        val collection: MongoCollection<Document> = mongoDatabase.getCollection("bank-account")
        val document = Document(mapOf(Pair("_id", UUID.randomUUID()),
            Pair("accountId", bankAccount.accountId),
            Pair("bankAccountId", bankAccount.bankAccountId)))
        collection.insertOne(document)
    }

    override fun findById(id: UUID): Optional<BankAccount> {
        val collection: MongoCollection<Document> = mongoDatabase.getCollection("bank-account")
        val document = collection.find(eq("bankAccountId", id)).first()
        document?: return Optional.empty()
        return Optional.of(BankAccount(document["bankAccountId"] as UUID, document["accountId"] as UUID))
    }

    override fun existsById(bankAccountId: UUID): Boolean {
        val collection: MongoCollection<Document> = mongoDatabase.getCollection("bank-account")
        collection.find(eq("bankAccountId", bankAccountId)).first() ?: return false
        return true
    }
}