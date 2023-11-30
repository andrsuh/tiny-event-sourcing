package jp.veka.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource

class HikariDatasourceProvider(databaseUrl: String, user: String, password: String) : DataSourceProvider {
    private val dataSource: DataSource
    init {
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = databaseUrl
        hikariConfig.username = user
        hikariConfig.password = password

        dataSource = HikariDataSource(hikariConfig)
    }
    override fun dataSource(): DataSource {
        return dataSource
    }
}