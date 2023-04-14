package ru.quipy.catalog.api

import java.beans.ConstructorProperties
import java.util.UUID

data class GetCatalogItemsDTO
@ConstructorProperties("title", "description", "price", "amount", "id")
constructor(val title: String, val description: String, val price: Int, val amount: Int, val id: UUID)