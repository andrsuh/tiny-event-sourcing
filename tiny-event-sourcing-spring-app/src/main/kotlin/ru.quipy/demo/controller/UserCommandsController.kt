package ru.quipy.demo.controller

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import ru.quipy.core.EventSourcingService
import ru.quipy.demo.domain.UserAggregate
import ru.quipy.demo.domain.addAddressCommand
import ru.quipy.demo.domain.createUserCommand
import ru.quipy.demo.domain.setDefaultAddressCommand
import java.util.*

@RestController
@RequestMapping("/user")
class UserCommandsController {
    @Autowired
    private lateinit var userEventSourcingService: EventSourcingService<UserAggregate>

    @PostMapping
    fun createUser(user: UserCreateDTO) {
        userEventSourcingService.update(UUID.randomUUID().toString()) {
            it.createUserCommand(user.userName, user.userPassword, user.userLogin)
        }
    }

    @PutMapping("{userId}/address/default/{addressId}")
    fun setDefaultAddress(
        @PathVariable addressId: UUID,
        @PathVariable userId: String
    ) {
        userEventSourcingService.update(userId) {
            it.setDefaultAddressCommand(addressId)
        }
    }

    @PostMapping("/{userId}/address")
    fun addAddress(
        @RequestBody body: AddAddressDTO,
        @PathVariable userId: String
    ) {
        userEventSourcingService.update(userId) {
            it.addAddressCommand(body.address)
        }
    }
}