package ru.quipy.core


class EventSourcingProperties (
    var snapshotFrequency: Int = 10,
    var snapshotTableName: String = "snapshots", // todo sukhoa should be per aggregate
    var streamReadPeriod: Long = 1_000,
    var streamBatchSize: Int = 200,
    var autoScanEnabled: Boolean = false,
    var scanPackage: String? = null,
    var spinLockMaxAttempts: Int = 25
)