package ru.quipy.user.api

import org.jetbrains.annotations.NotNull

data class UserRegisterDTO (
    @field:NotNull
    var email: String,
    var password: String,
)