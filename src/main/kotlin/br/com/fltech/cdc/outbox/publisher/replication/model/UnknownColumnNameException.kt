package br.com.fltech.cdc.outbox.publisher.replication.model

class UnknownColumnNameException(columnName: String) : Exception("Unknown column name $columnName")
