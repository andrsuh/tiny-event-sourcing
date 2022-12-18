package ru.quipy.eventstore.factory

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoDatabase

interface MongoClientFactory {
    fun getDatabase(): MongoDatabase
    fun getClient(): MongoClient
}