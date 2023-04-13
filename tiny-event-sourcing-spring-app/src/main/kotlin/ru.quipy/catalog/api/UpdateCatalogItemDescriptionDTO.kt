package ru.quipy.catalog.api

import java.beans.ConstructorProperties

data class UpdateCatalogItemDescriptionDTO
@ConstructorProperties("title", "description")
constructor(val title: String, val description: String)