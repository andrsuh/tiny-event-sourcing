package ru.quipy.eventstore.converter

import java.util.UUID

class UuidConverter : BsonConverter<UUID, List<String?>> {

    override fun convertFromBsonType(value: Any): List<String?>? {
        if (value !is UUID) return null
        return listOf(
            UUID::class.qualifiedName,
            value.toString()
        )
    }

    override fun convertToBsonType(value: Any): UUID? {
        if (value !is List<*>
            || value.size != 2
            || value[1] !is String
        ) return null
        if (UUID::class.qualifiedName != value[0]) return null
        return UUID.fromString(value[1] as String)
    }

}