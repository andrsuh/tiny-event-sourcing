package ru.quipy.core.exceptions

class DuplicateEventIdException(
    override val message: String,
    override val cause: Throwable?
) : RuntimeException(
    message, cause
)