package ru.quipy.catalog.api

import java.beans.ConstructorProperties

data class CreateCatalogItemDTO
@ConstructorProperties("title", "description", "price", "amount")
constructor(val title: String, val description: String, val price: Int, val amount: Int)