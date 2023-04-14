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
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.ArrayList
import java.lang.IllegalStateException

@RestController()
@RequestMapping("/catalog")
class CatalogItemController (
        val catalogItemESService: EventSourcingService<UUID, CatalogAggregate, CatalogAggregateState>,
        val catalogItemRepository: CatalogItemRepository,
){

    @PostMapping()
    fun createItem(@RequestBody request: CreateCatalogItemDTO): Any {
        if (request.price < 0 || request.amount < 0){
            return ResponseEntity<Any>(null, HttpStatus.UNPROCESSABLE_ENTITY)
        }
        if (catalogItemRepository.findOneByTitle(request.title) != null) {
            return ResponseEntity<Any>(null, HttpStatus.CONFLICT)
        }
        val itemId =  catalogItemESService.create{ it.createItem(
                description = request.description,
                price = request.price,
                amount = request.amount
        )}.itemId

        return catalogItemRepository.save(CatalogItemMongo(
                title = request.title,
                description =  request.description,
                price = request.price,
                amount = request.amount,
                aggregateId = itemId
        ))
    }

    @PatchMapping("/price/{id}")
    fun updateItemPrice(@PathVariable id: UUID, @RequestBody request: UpdateCatalogItemPriceDTO): Any {
        if (request.price < 0){
            return ResponseEntity<Any>(null, HttpStatus.UNPROCESSABLE_ENTITY)
        }
        if (catalogItemRepository.findOneByTitle(request.title) == null) {
            return ResponseEntity<Any>(null, HttpStatus.BAD_REQUEST)
        }
        return catalogItemESService.update(id){it.updateItemPrice(id = id, request.price)}
    }

    @PatchMapping("/description/{id}")
    fun updateItemPrice(@PathVariable id: UUID, @RequestBody catalogItemDTO: UpdateCatalogItemDescriptionDTO): Any {
        if (catalogItemRepository.findOneByTitle(catalogItemDTO.title) == null) {
            return ResponseEntity<Any>(null, HttpStatus.BAD_REQUEST)
        }

        return catalogItemESService.update(id){it.updateItemDescription(id = id, catalogItemDTO.description)}
    }

    private val mutexAddAmount = ReentrantLock()
    @PatchMapping("/amount/{id}")
    fun updateAmountPrice(@PathVariable id: UUID, @RequestBody catalogItemDTO: UpdateCatalogItemAmountDTO): Any {
        if (catalogItemDTO.amount < 0){
            return ResponseEntity<Any>("Error: amount cannot be below zero", HttpStatus.BAD_REQUEST)
        }
        if (catalogItemRepository.findOneByTitle(catalogItemDTO.title) == null) {
            return ResponseEntity<Any>(null, HttpStatus.BAD_REQUEST)
        }
        mutexAddAmount.lock()
        try{
            return catalogItemESService.update(id){it.updateItemAmount(id = id, catalogItemDTO.amount)}
        }finally {
            mutexAddAmount.unlock()
        }
    }

    private val mutexBuy = ReentrantLock()
    @PatchMapping("/buy/{id}")
    fun buyItem(@PathVariable id: UUID, @RequestBody request: UpdateCatalogItemAmountDTO): Any {
        if (request.amount <= 0){
            return ResponseEntity<Any>("Error: Amount cannot be below zero or equal zero", HttpStatus.UNPROCESSABLE_ENTITY)
        }
        if (catalogItemRepository.findOneByTitle(request.title) == null) {
            return ResponseEntity<Any>(null, HttpStatus.BAD_REQUEST)
        }
        mutexBuy.lock()

        try{
            return catalogItemESService.update(id){it.removeItemAmount(id, request.amount)}
        }
        catch(e: IllegalStateException){
            return ResponseEntity<Any>("Error ${e.message}",HttpStatus.BAD_REQUEST)
        }
        finally {
            mutexBuy.unlock()
        }
    }

    private val mutexReturn = ReentrantLock()
    @PatchMapping("/return")
    fun returnItems(@RequestBody request: UpdateCatalogItemAmountListDTO): Any {
        if (request.titles.size != request.ids.size && request.ids.size != request.amounts.size){
            return ResponseEntity<Any>(null, HttpStatus.UNPROCESSABLE_ENTITY)
        }

        mutexReturn.lock()
        try {
            for (i in 0 until request.titles.size){
                val title = request.titles[i]
                val id = request.ids[i]
                val backAmount = request.amounts[i]
                if (backAmount <= 0) continue
                if (catalogItemRepository.findOneByTitle(title) != null){
                    catalogItemESService.update(id){it.addItemAmount(id, backAmount)}
                }
            }

            return  ResponseEntity<Any>("", HttpStatus.OK)
        }finally {
            mutexReturn.unlock()
        }
    }

    @GetMapping
    fun getItem(): Any {
        val result = ArrayList<GetCatalogItemsDTO>()
        for (item in catalogItemRepository.findAll()) {
            result.add(
                    GetCatalogItemsDTO(
                            title = item.title,
                            description = catalogItemESService.getState(item.aggregateId)!!.getDescription()!!,
                            price = catalogItemESService.getState(item.aggregateId)!!.getPrice()!!,
                            amount = catalogItemESService.getState(item.aggregateId)!!.getAmount()!!,
                            id = catalogItemESService.getState(item.aggregateId)!!.getId()!!
                    )
            )
        }

        return result
    }
}