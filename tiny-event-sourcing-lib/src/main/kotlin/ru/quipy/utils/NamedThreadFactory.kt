package ru.quipy.utils

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class NamedThreadFactory(private val prefix: String) : ThreadFactory {
    private val sequence = AtomicInteger(1)

    override fun newThread(r: Runnable): Thread {
        val thread = Thread(r)
        val seq = sequence.getAndIncrement()
        thread.name = prefix + (if (seq > 1) "-$seq" else "")
        thread.isDaemon = true
        return thread
    }
}
