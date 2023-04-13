package ru.quipy.user.api

import java.beans.ConstructorProperties

data class UserAuthDTO
@ConstructorProperties("email", "password")
constructor(val email: String, val password: String)