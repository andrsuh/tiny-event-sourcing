package ru.quipy.db

import javax.sql.DataSource

interface DataSourceProvider {
    fun dataSource() : DataSource
}