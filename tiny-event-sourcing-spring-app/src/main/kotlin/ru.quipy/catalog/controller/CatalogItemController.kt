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

        if (catalogItemDTO.amount < 0){
            return ResponseEntity<Any>("Error: amount cannot be below zero", HttpStatus.BAD_REQUEST)
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
        if (left < 0){
            return ResponseEntity<Any>("Error: there is no so much items", HttpStatus.BAD_REQUEST)
        }

        return catalogItemESService.update(id){it.updateItemAmount(id = id, left)}
    }

    @PatchMapping("/return")
    fun returnItems(@RequestBody catalogItemDTO: UpdateCatalogItemAmountListDTO): Any {
        if (catalogItemDTO.titles.size != catalogItemDTO.ids.size && catalogItemDTO.ids.size != catalogItemDTO.amounts.size){
            return ResponseEntity<Any>(null, HttpStatus.UNPROCESSABLE_ENTITY)
        }

        for (i in 0 until catalogItemDTO.titles.size){
            val title = catalogItemDTO.titles[i]
            val id = catalogItemDTO.ids[i]
            val backAmount = catalogItemDTO.amounts[i]
            if (catalogItemRepository.findOneByTitle(title) != null){
                val amount =
                        if (catalogItemESService.getState(id)!!.getAmount() != null)
                            catalogItemESService.getState(id)!!.getAmount()
                        else
                            catalogItemRepository.findOneByTitle(title).amount
                val left = amount!! + backAmount.toInt()
                catalogItemESService.update(id){it.updateItemAmount(id = id, left)}
            }
        }

        return  ResponseEntity<Any>("", HttpStatus.OK)
    }

    @GetMapping
    fun getItem(): Any {
        val result = ArrayList<GetCatalogItemsDTO>()
        for (item in catalogItemRepository.findAll()) {
            result.add(
                    GetCatalogItemsDTO(
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
                            id = catalogItemESService.getState(item.aggregateId)!!.getId()!!
                    )
            )
        }

        return result
    }
}