package ru.quipy.storage.exception

open class MongoClientException(
    override val message: String?,
    override val cause: Throwable?
) : RuntimeException(
    message, cause
)
