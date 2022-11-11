package ru.quipy.core.exceptions

import ru.quipy.domain.EventRecord

internal class EventRecordOptimisticLockException(
    override val message: String,
    override val cause: Throwable?,
    val eventRecord: EventRecord
) : RuntimeException(
    message, cause
)