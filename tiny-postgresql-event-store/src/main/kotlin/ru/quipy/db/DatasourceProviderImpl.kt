package ru.quipy.db

import javax.sql.DataSource

class DatasourceProviderImpl(private val dataSource: DataSource) : DataSourceProvider {
    override fun dataSource(): DataSource {
        return dataSource
    }
}