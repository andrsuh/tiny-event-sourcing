package ru.quipy.catalog.api

import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate

@AggregateType(aggregateEventsTableName = "catalog")
class CatalogAggregate: Aggregate