package jp.veka.query

import java.sql.Connection

interface Query  {
    fun build() : String
}