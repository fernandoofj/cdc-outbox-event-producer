package br.com.fltech.outbox.publisher.replication.config

object PostgresConfigurationMother {
    fun build() =
        PostgresConfiguration(
            host = "localhost",
            database = "catalogue",
            username = "user",
            password = "password",
        )
}
