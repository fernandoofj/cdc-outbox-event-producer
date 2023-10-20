package shop.inventa.pg2sns4k.replication.model

class UnknownColumnNameException(columnName: String) : Exception("Unknown column name $columnName")
