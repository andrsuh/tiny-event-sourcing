package ru.quipy.storage.exception

open class MongoDuplicateKeyException(
    override val message: String?,
    override val cause: Throwable?
) : MongoClientException(
    message, cause
) {
    constructor(cause: Throwable?) : this(null, cause)
}