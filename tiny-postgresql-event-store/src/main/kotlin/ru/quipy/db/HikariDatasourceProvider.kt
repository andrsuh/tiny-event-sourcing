package ru.quipy.db

import com.zaxxer.hikari.HikariDataSource

class HikariDatasourceProvider(private val datasource: HikariDataSource) : DataSourceProvider {
    override fun dataSource(): HikariDataSource {
        return datasource
    }
}