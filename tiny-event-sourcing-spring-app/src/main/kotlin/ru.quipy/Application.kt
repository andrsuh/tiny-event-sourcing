package ru.quipy

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories

@EnableMongoRepositories
@SpringBootApplication
class Application

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}
