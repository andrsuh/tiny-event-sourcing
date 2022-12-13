package ru.quipy.eventstore.exception

import com.mongodb.*

class MongoClientExceptionTranslator {

    private fun translateException(ex: Exception): MongoClientException? {
        when (ex) {
            is DuplicateKeyException -> {
                return MongoDuplicateKeyException(ex.message, ex)
            }
            is MongoWriteException -> {
                if (ErrorCategory.fromErrorCode(ex.code) == ErrorCategory.DUPLICATE_KEY) {
                    return MongoDuplicateKeyException(ex.message, ex)
                }
            }
            is MongoBulkWriteException -> {
                for (x in ex.writeErrors) {
                    if (x.code == 11000) {
                        return MongoDuplicateKeyException(ex.message, ex)
                    }
                }
            }
            is MongoServerException -> {
                if (ex.code == 11000) {
                    return MongoDuplicateKeyException(ex.message, ex)
                }
            }
        }
        return null
    }

    fun <T> withTranslation(action: () -> T): T {
        try {
            return action()
        } catch (ex: Exception) {
            throw this.translateException(ex) ?: ex
        }
    }
}