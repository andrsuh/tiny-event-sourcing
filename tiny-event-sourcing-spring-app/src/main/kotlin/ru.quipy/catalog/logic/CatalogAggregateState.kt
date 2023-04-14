package ru.quipy.catalog.logic

import ru.quipy.catalog.api.*
import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import java.lang.IllegalStateException
import java.util.UUID

class CatalogAggregateState: AggregateState<UUID, CatalogAggregate> {
    private lateinit var itemId: UUID
    private var description: String? = null
    private var amount: Int? = null
    private var price: Int? = null

    override fun getId(): UUID? = itemId
    fun getDescription(): String? = description
    fun getAmount(): Int? = amount!!
    fun getPrice(): Int? = price!!

    fun resetField(target: Any, fieldName: String) {
        val field = target.javaClass.getDeclaredField(fieldName)

        with (field) {
            isAccessible = true
            set(target, null)
        }
    }

    fun createItem(id: UUID = UUID.randomUUID(), description: String, amount: Int, price: Int) : ItemCreatedEvent =
            ItemCreatedEvent(id, description, price, amount)
    fun removeItem(id: UUID): ItemRemovedEvent =
            ItemRemovedEvent(id)
    fun updateItemPrice(id: UUID, price: Int): ItemUpdatedPriceEvent =
            ItemUpdatedPriceEvent(id, price)
    fun updateItemAmount(id: UUID, amount: Int): ItemUpdatedAmountEvent =
            ItemUpdatedAmountEvent(id, amount)
    fun updateItemDescription(id: UUID, description: String): ItemUpdatedDescriptionEvent =
        ItemUpdatedDescriptionEvent(id, description)

    fun removeItemAmount(id: UUID, amount: Int): ItemRemovedAmountEvent {
        if (this.amount!! < amount) throw IllegalStateException("Buy more than avaible")
        return ItemRemovedAmountEvent(id, amount)
    }

    fun addItemAmount(id: UUID, amount: Int): ItemAddAmountEvent {
        println("2")
        return ItemAddAmountEvent(id, amount)
    }

    @StateTransitionFunc
    fun removeItemAmount(event: ItemRemovedAmountEvent){
        itemId = event.itemId
        if (amount?.minus(event.amount)!! < 0)
            amount = 0
        else
            amount = amount?.minus(event.amount)
    }

    @StateTransitionFunc
    fun addItemAmount(event: ItemAddAmountEvent){
        itemId = event.itemId
        amount = amount?.plus(event.amount)
    }

    @StateTransitionFunc
    fun createItem(event: ItemCreatedEvent){
        itemId = event.itemId
        description = event.description
        price = event.price
        amount = event.amount
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