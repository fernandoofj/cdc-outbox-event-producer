package br.com.fltech.cdc.outbox.publisher.replication.config

object ReplicationConfigurationMother {

    fun build() = ReplicationConfiguration(
        slotName = "slot-name"
    )
}
