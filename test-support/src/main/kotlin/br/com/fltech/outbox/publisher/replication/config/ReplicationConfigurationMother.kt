package br.com.fltech.outbox.publisher.replication.config

object ReplicationConfigurationMother {
    fun build() =
        ReplicationConfiguration(
            slotName = "slot-name",
        )
}
