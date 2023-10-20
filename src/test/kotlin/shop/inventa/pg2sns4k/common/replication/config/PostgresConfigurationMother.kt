package shop.inventa.pg2sns4k.common.replication.config

object PostgresConfigurationMother {
    fun build() = PostgresConfiguration(
        host = "localhost",
        database = "catalogue",
        username = "user",
        password = "password"
    )
}
