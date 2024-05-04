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
        val document = Document()
        document["bankAccountId"] = bankAccount.bankAccountId
        document["accountId"] = bankAccount.accountId
        collection.insertOne(document)
    }

    override fun findById(id: UUID): Optional<BankAccount> {
        val collection: MongoCollection<Document> = mongoDatabase.getCollection("bank-account")
        val searchQuery = Document()
        searchQuery["accountId"] = id
        val document = collection.find(eq("accountId", id)).first() ?: return Optional.empty()
        return Optional.of(BankAccount(document["bankAccountId"] as UUID, document["accountId"] as UUID))
    }
}