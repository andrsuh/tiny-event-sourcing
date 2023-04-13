package ru.quipy.catalog.logic

import ru.quipy.catalog.api.*
import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import java.util.UUID

class CatalogAggregateState: AggregateState<UUID, CatalogAggregate> {
    private lateinit var itemId: UUID
    private var description: String? = null
    private var amount: Int? = null
    private var price: Int? = null

    override fun getId(): UUID? = itemId
    fun getDescription(): String? = description
    fun getAmount(): Int? = amount
    fun getPrice(): Int? = price

    fun resetField(target: Any, fieldName: String) {
        val field = target.javaClass.getDeclaredField(fieldName)

        with (field) {
            isAccessible = true
            set(target, null)
        }
    }

    fun createItem(id: UUID = UUID.randomUUID()) : ItemCreatedEvent = ItemCreatedEvent(id)
    fun removeItem(id: UUID): ItemRemovedEvent = ItemRemovedEvent(id)
    fun updateItemPrice(id: UUID, price: Int): ItemUpdatedPriceEvent =
            ItemUpdatedPriceEvent(id, price)
    fun updateItemAmount(id: UUID, amount: Int): ItemUpdatedAmountEvent =
            ItemUpdatedAmountEvent(id, amount)
    fun updateItemDescription(id: UUID, description: String): ItemUpdatedDescriptionEvent =
            ItemUpdatedDescriptionEvent(id, description)

    @StateTransitionFunc
    fun createItem(event: ItemCreatedEvent){
        itemId = event.itemId
    }

    @StateTransitionFunc
    fun removeItem(event: ItemRemovedEvent){
        resetField(this, "itemId")
    }

    @StateTransitionFunc
    fun updatePriceItem(event: ItemUpdatedPriceEvent){
        itemId = event.itemId
        price = event.price
    }

    @StateTransitionFunc
    fun updateDescriptionItem(event: ItemUpdatedDescriptionEvent) {
        itemId = event.itemId
        description = event.description
    }

    @StateTransitionFunc
    fun updateAmountItem(event: ItemUpdatedAmountEvent){
        itemId = event.itemId
        amount = event.amount
    }
}