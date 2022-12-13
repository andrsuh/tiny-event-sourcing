package ru.quipy.eventstore.exception

open class MongoClientException(
    override val message: String?,
    override val cause: Throwable?
) : RuntimeException(
    message, cause
)
