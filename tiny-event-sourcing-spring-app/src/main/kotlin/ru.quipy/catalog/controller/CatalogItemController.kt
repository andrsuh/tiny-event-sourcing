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

@RestController("/catalog")
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
    fun updateItemPrice(@PathVariable id: UUID, @RequestBody catalogItemDTO: UpdateCatalogItemAmountDTO): Any {
        if (catalogItemRepository.findOneByTitle(catalogItemDTO.title) == null) {
            return ResponseEntity<Any>(null, HttpStatus.BAD_REQUEST)
        }

        return catalogItemESService.update(id){it.updateItemAmount(id = id, catalogItemDTO.amount)}
    }

}