package ru.quipy

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
@SpringBootApplication
class SagasProjectionsApplication

fun main(args: Array<String>) {
    runApplication<SagasProjectionsApplication>(*args)
}
