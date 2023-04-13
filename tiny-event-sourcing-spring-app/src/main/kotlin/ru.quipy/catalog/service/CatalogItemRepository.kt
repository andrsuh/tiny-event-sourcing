package ru.quipy.catalog.service

import org.springframework.data.mongodb.repository.MongoRepository

interface CatalogItemRepository: MongoRepository<CatalogItemMongo, String> {

    @org.springframework.lang.Nullable
    fun findOneByTitle(title: String): CatalogItemMongo
}