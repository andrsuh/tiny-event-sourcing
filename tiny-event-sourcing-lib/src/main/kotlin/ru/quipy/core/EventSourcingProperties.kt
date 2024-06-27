package ru.quipy.core

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class EventSourcingProperties (
    var snapshotFrequency: Int = 10,
    var snapshotsEnabled: Boolean = false,
    var snapshotTableName: String = "snapshots", // todo sukhoa should be per aggregate
    var streamReadPeriod: Long = 1_000,
    var streamBatchSize: Int = 200,
    var autoScanEnabled: Boolean = false,
    var scanPackage: String? = null,
    var spinLockMaxAttempts: Int = 25,
    var maxActiveReaderInactivityPeriod: Duration = 5.seconds,
    var readerCommitPeriodMessages: Int = 100,
    var readerCommitPeriodMillis: Long = 1000,
    val eventReaderHealthCheckPeriod: Duration = 3.seconds,
    var sagasEnabled: Boolean = true,
    var batchEnabled: Boolean = false,
    var batchSize: Int = 1,
    var batchPeriodMillis: Long = 50,
    var batchMode: String = BatchMode.STORED_PROCEDURE.name,
)

enum class BatchMode {
    JDBC_BATCH, STORED_PROCEDURE
}