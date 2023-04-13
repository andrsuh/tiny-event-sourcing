package ru.quipy.catalog.api

import java.beans.ConstructorProperties

data class UpdateCatalogItemAmountDTO
@ConstructorProperties("title", "amount")
constructor(val title: String, val amount: Int)