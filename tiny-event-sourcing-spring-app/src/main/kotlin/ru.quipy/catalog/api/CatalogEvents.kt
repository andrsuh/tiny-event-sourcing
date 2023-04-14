package ru.quipy.catalog.api

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import java.util.*

const val ITEM_CREATED = "ITEM_CREATED"
const val ITEM_REMOVED = "ITEM_REMOVED"
const val ITEM_UPDATED_PRICE = "ITEM_UPDATED_PRICE"
const val ITEM_UPDATED_DESCRIPTION = "ITEM_UPDATED_DESCRIPTION"
const val ITEM_UPDATED_AMOUNT = "ITEM_UPDATED_AMOUNT"
const val ITEM_REMOVED_AMOUNT = "ITEM_REMOVED_AMOUNT"
const val ITEM_ADD_AMOUNT = "ITEM_ADD_AMOUNT"

@DomainEvent(name = ITEM_REMOVED_AMOUNT)
data class ItemRemovedAmountEvent(
        val itemId: UUID,
        val amount: Int
): Event<CatalogAggregate>(
        name = ITEM_REMOVED_AMOUNT
)

@DomainEvent(name = ITEM_ADD_AMOUNT)
data class ItemAddAmountEvent(
        val itemId: UUID,
        val amount: Int
): Event<CatalogAggregate>(
        name = ITEM_ADD_AMOUNT
)

@DomainEvent(name = ITEM_CREATED)
data class ItemCreatedEvent(
        val itemId: UUID,
        val description: String,
        val price: Int,
        val amount: Int
) : Event<CatalogAggregate>(
        name = ITEM_CREATED
)

@DomainEvent(name = ITEM_REMOVED)
data class ItemRemovedEvent(
        val itemId: UUID,
) : Event<CatalogAggregate>(
        name = ITEM_REMOVED
)

@DomainEvent(name = ITEM_UPDATED_PRICE)
data class ItemUpdatedPriceEvent(
        val itemId: UUID,
        val price: Int,
) : Event<CatalogAggregate> (
        name = ITEM_UPDATED_PRICE
)

@DomainEvent(name = ITEM_UPDATED_DESCRIPTION)
data class ItemUpdatedDescriptionEvent(
        val itemId: UUID,
        val description: String,
) : Event<CatalogAggregate> (
        name = ITEM_UPDATED_DESCRIPTION
)

@DomainEvent(name = ITEM_UPDATED_AMOUNT)
data class ItemUpdatedAmountEvent(
        val itemId: UUID,
        val amount: Int,
) : Event<CatalogAggregate> (
        name = ITEM_UPDATED_AMOUNT
)