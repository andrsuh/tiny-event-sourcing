package ru.quipy.demo

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.quipy.core.EventSourcingService
import java.util.*

@RestController
@RequestMapping("/user")
class UserCommandsController {
    @Autowired
    private lateinit var userEventSourcingService: EventSourcingService<UserAggregate>

    @PostMapping
    fun createUser(user: UserCreateDTO) {
        this.userEventSourcingService.update(UUID.randomUUID().toString()) {
            it.createUserCommand(user.userName, user.userPassword, user.userLogin)
        }
    }

    @PutMapping("{userId}/address/default/{addressId}")
    fun setDefaultAddress(
        @PathVariable addressId: UUID,
        @PathVariable userId: String
    ) {
        this.userEventSourcingService.update(userId) {
            it.setDefaultAddressCommand(addressId)
        }
    }

    @PostMapping("/{userId}/address")
    fun addAddress(
        @RequestBody body: AddAddressDTO,
        @PathVariable userId: String
    ) {
        this.userEventSourcingService.update(userId) {
            it.addAddressCommand(body.address)
        }
    }
}