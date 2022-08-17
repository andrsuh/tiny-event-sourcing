package ru.quipy.demo.controller


data class UserCreateDTO(
    val userLogin: String,
    val userPassword: String,
    val userName: String
)
