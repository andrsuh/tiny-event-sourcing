package ru.quipy.catalog.service

import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.FieldType
import org.springframework.data.mongodb.core.mapping.MongoId
import java.util.UUID

@Document
data class CatalogItemMongo (
        @MongoId(value = FieldType.STRING)
        val title: String,
        val description: String,
        val price: Int,
        val amount: Int,
        val aggregateId: UUID,
)