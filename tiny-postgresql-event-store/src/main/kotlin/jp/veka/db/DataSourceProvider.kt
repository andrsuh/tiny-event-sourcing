package jp.veka.db

import javax.sql.DataSource

interface DataSourceProvider {
    fun dataSource() : DataSource
}