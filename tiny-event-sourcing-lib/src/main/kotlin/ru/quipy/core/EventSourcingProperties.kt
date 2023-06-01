package ru.quipy.core

import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class EventSourcingProperties (
    var snapshotFrequency: Int = 10,
    var snapshotTableName: String = "snapshots", // todo sukhoa should be per aggregate
    var streamReadPeriod: Long = 1_000,
    var streamBatchSize: Int = 200,
    var autoScanEnabled: Boolean = false,
    var scanPackage: String? = null,
    var spinLockMaxAttempts: Int = 25,
    var maxActiveReaderInactivityPeriod: Duration = 5.minutes,
    var recordReadIndexCommitPeriod: Int = 10,
    val eventReaderHealthCheckPeriod: Duration = 5.seconds,
    var sagasEnabled: Boolean = true
)
