package ru.quipy.catalog.api
import java.beans.ConstructorProperties
data class UpdateCatalogItemPriceDTO
@ConstructorProperties("title","price")
constructor(val title: String,val price: Int)