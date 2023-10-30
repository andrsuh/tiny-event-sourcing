package jp.veka.tables

class Column<T> {
    val index: Int
    val name: String
    val type: Class<T>

    constructor(index: Int, name: String, type: Class<T>) {
        this.index = index
        this.name = name
        this.type = type
    }
}