package ru.quipy.catalog.api

import java.beans.ConstructorProperties
import java.util.UUID

data class UpdateCatalogItemAmountListDTO
@ConstructorProperties("titles", "ids","amounts")
constructor(val titles: List<String>, val ids: List<UUID>, val amounts: List<Int>)