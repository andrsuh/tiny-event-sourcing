package ru.quipy.query

import java.sql.Connection

interface Query  {
    fun build() : String
}