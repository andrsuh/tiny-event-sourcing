package ru.quipy.demo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.quipy.core.EventSourcingService
import java.util.*

@RestController
@RequestMapping("/user")
class UserCommandsController {
    @Autowired
    private lateinit var demoESService: EventSourcingService<UserAggregate>

    @PostMapping
    fun createUser(user: UserCreatedDTO) {
        this.demoESService.update(UUID.randomUUID().toString()) {
            it.createUserCommand(user.userName, user.userPassword, user.userLogin)
        }
    }

    @PostMapping("/Test")
    fun createUser() {
        this.demoESService.update("1") {
            it.createUserCommand("Ivan", "12345", "VANYA12345")
        }
    }

    @GetMapping("/User/{userId}/SetAddress/{addressId}")
    fun setDefaultAddress(
            @PathVariable addressId: UUID,
            @PathVariable userId: String) {
        this.demoESService.update(userId) {
            it.setDefaultAddressCommand(addressId)
        }
    }

    @GetMapping("/User/{userId}/address")
    fun addAddress(
            @RequestBody body: AddAddressDTO,
            @PathVariable userId: String
    ) {
        this.demoESService.update(userId) {
            it.addAddressCommand(body.address)
        }
    }
}