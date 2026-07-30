package br.com.fltech.outbox.publisher.replication.model

class UnknownColumnNameException(columnName: String) : Exception("Unknown column name $columnName")
