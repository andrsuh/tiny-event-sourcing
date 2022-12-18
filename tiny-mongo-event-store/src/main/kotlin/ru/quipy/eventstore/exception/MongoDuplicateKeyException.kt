package ru.quipy.eventstore.exception

open class MongoDuplicateKeyException(
    override val message: String?,
    override val cause: Throwable?
) : MongoClientException(
    message, cause
) {
    constructor(cause: Throwable?) : this(null, cause)
}