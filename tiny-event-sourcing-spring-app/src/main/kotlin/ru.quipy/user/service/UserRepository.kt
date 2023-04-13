package ru.quipy.user.service

import org.springframework.data.mongodb.repository.MongoRepository

interface UserRepository: MongoRepository<UserMongo, String> {
    @org.springframework.lang.Nullable
    fun findOneByEmail(email: String): UserMongo
}