package ru.quipy.database

import ru.quipy.domain.EventAggregation

interface OngoingGroupStorage {

    fun insertEventAggregation(tableName: String, eventAggregation: EventAggregation)

    fun findBatchOfEventAggregations(
        tableName: String
    ): List<EventAggregation>

    fun dropTable(
        tableName: String
    )
}
