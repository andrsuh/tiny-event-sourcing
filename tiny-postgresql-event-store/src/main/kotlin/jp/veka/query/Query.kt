package jp.veka.query

import java.sql.Connection

interface Query  {
    fun execute(connection: Connection) : Any // TODO remove
    fun build() : String
}