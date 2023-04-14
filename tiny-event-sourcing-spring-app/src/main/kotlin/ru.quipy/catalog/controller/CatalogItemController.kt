package ru.quipy.catalog.controller

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.catalog.api.*
import ru.quipy.catalog.logic.CatalogAggregateState
import ru.quipy.catalog.service.CatalogItemMongo
import ru.quipy.catalog.service.CatalogItemRepository
import ru.quipy.core.EventSourcingService
import java.util.*
import kotlin.collections.ArrayList

@RestController()
@RequestMapping("/catalog")
class CatalogItemController (
        val catalogItemESService: EventSourcingService<UUID, CatalogAggregate, CatalogAggregateState>,
        val catalogItemRepository: CatalogItemRepository,
){

    @PostMapping()
    fun createItem(@RequestBody catalogItemDTO: CreateCatalogItemDTO): Any {
        if (catalogItemRepository.findOneByTitle(catalogItemDTO.title) != null) {
            return ResponseEntity<Any>(null, HttpStatus.CONFLICT)
        }
        return catalogItemRepository.save(CatalogItemMongo(
                title = catalogItemDTO.title,
                description =  catalogItemDTO.description,
                price = catalogItemDTO.price,
                amount = catalogItemDTO.amount,
                aggregateId = catalogItemESService.create { it.createItem() }.itemId
        ))
    }

    @PatchMapping("/price/{id}")
    fun updateItemPrice(@PathVariable id: UUID, @RequestBody catalogItemDTO: UpdateCatalogItemPriceDTO): Any {
        if (catalogItemRepository.findOneByTitle(catalogItemDTO.title) == null) {
            return ResponseEntity<Any>(null, HttpStatus.BAD_REQUEST)
        }
        return catalogItemESService.update(id){it.updateItemPrice(id = id, catalogItemDTO.price)}
    }

    @PatchMapping("/description/{id}")
    fun updateItemPrice(@PathVariable id: UUID, @RequestBody catalogItemDTO: UpdateCatalogItemDescriptionDTO): Any {
        if (catalogItemRepository.findOneByTitle(catalogItemDTO.title) == null) {
            return ResponseEntity<Any>(null, HttpStatus.BAD_REQUEST)
        }

        return catalogItemESService.update(id){it.updateItemDescription(id = id, catalogItemDTO.description)}
    }

    @PatchMapping("/amount/{id}")
    fun updateAmountPrice(@PathVariable id: UUID, @RequestBody catalogItemDTO: UpdateCatalogItemAmountDTO): Any {
        if (catalogItemRepository.findOneByTitle(catalogItemDTO.title) == null) {
            return ResponseEntity<Any>(null, HttpStatus.BAD_REQUEST)
        }

        return catalogItemESService.update(id){it.updateItemAmount(id = id, catalogItemDTO.amount)}
    }

    @PatchMapping("/buy/{id}")
    fun buyItem(@PathVariable id: UUID, @RequestBody catalogItemDTO: UpdateCatalogItemAmountDTO): Any {
        if (catalogItemRepository.findOneByTitle(catalogItemDTO.title) == null) {
            return ResponseEntity<Any>(null, HttpStatus.BAD_REQUEST)
        }

        val amount =
                if (catalogItemESService.getState(id)!!.getAmount() != null)
                    catalogItemESService.getState(id)!!.getAmount()
                else
                    catalogItemRepository.findOneByTitle(catalogItemDTO.title).amount
        val left = amount!! - catalogItemDTO.amount

        return catalogItemESService.update(id){it.updateItemAmount(id = id, left)}
    }

    @GetMapping
    fun getItem(): Any {
        val result = ArrayList<CreateCatalogItemDTO>()
        for (item in catalogItemRepository.findAll()) {
            result.add(
                    CreateCatalogItemDTO(
                            title = item.title,
                            description =
                            if (catalogItemESService.getState(item.aggregateId)!!.getDescription() != null)
                                catalogItemESService.getState(item.aggregateId)!!.getDescription()!!
                            else item.title,
                            price = if (catalogItemESService.getState(item.aggregateId)!!.getPrice() != null)
                                catalogItemESService.getState(item.aggregateId)!!.getPrice()!!
                            else item.price,
                            amount = if (catalogItemESService.getState(item.aggregateId)!!.getAmount() != null)
                                catalogItemESService.getState(item.aggregateId)!!.getAmount()!!
                            else item.amount,
                    )
            )
        }

        return result
    }
}