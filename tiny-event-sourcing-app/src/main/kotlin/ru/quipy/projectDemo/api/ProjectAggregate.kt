package ru.quipy.projectDemo.api

import ru.quipy.core.annotations.AggregateType
import ru.quipy.domain.Aggregate

// API
@AggregateType(aggregateEventsTableName = "aggregate-project")
class ProjectAggregate : Aggregate