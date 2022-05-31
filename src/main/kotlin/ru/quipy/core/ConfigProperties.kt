package ru.quipy.core

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "event.sourcing") // todo sukhoa spring-specific
class ConfigProperties {
    var snapshotFrequency: Int = 10
    var snapshotTableName: String = "snapshots"
    var streamReadPeriod: Long = 1_000
    var streamBatchSize: Int = 200
}