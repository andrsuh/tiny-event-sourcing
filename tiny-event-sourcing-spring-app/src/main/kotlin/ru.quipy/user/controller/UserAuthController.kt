package ru.quipy.user.controller

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RestController
import ru.quipy.core.EventSourcingService
import ru.quipy.user.api.UserAggregate
import ru.quipy.user.logic.UserAggregateState
import ru.quipy.user.service.UserRepository
import java.util.*
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import ru.quipy.user.api.UserRegisterDTO
import ru.quipy.user.service.UserMongo

@RestController
class UserAuthController (
    val userESService: EventSourcingService<UUID, UserAggregate, UserAggregateState>,
    val usersRepository: UserRepository,
    val passwordEncoder: BCryptPasswordEncoder
) {
    @PostMapping("/auth/create")
    fun createUser(@RequestBody userRegisterDTO: UserRegisterDTO): Any {
        if (usersRepository.findOneByEmail(userRegisterDTO.email) != null) {
            return ResponseEntity<Any>(null, HttpStatus.CONFLICT)
        }
        return usersRepository.save(
            UserMongo(
            email = userRegisterDTO.email,
            password =  passwordEncoder.encode(userRegisterDTO.password),
            aggregateId = userESService.create { it.createUser() }.userId,
            role = "user"
        )
        )
    }
}