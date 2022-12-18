package ru.quipy.eventstore.factory

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import org.bson.UuidRepresentation

class MongoClientFactoryImpl : MongoClientFactory {
    private val client: MongoClient
    private val databaseName: String

    constructor(databaseName: String) : this("mongodb://localhost", databaseName)

    constructor(connectionString: String, databaseName: String)
            : this(ConnectionString(connectionString), databaseName)

    constructor(connectionString: ConnectionString, databaseName: String) {
        this.client = MongoClients.create(
            MongoClientSettings.builder()
                .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
                .applyConnectionString(connectionString)
                .build()
        )
        this.databaseName = databaseName
    }

    constructor(mongoClient: MongoClient, databaseName: String) {
        this.client = mongoClient
        this.databaseName = databaseName
    }

    override fun getDatabase(): MongoDatabase {
        return client.getDatabase(databaseName)
    }


    override fun getClient(): MongoClient {
        return this.client
    }
}