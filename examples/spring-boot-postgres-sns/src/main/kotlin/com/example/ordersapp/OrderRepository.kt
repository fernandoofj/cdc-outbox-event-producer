package com.example.ordersapp

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

@Repository
interface OrderRepository : JpaRepository<Order, Long> {
    /**
     * `pg_logical_emit_message` is the producer-side half of the outbox
     * contract: it writes a WAL message inside the *same* transaction as
     * the row insert above it, so the message and the row either both
     * commit or neither does. `cdc-outbox-source-postgres` reads it back
     * off the replication slot and routes it by the `sns://` prefix.
     *
     * Not `@Modifying`: `pg_logical_emit_message` is a SELECT returning
     * one scalar row, not an UPDATE/DELETE — Spring Data JPA on Boot 4
     * rejects `@Modifying` methods whose return type isn't
     * `void`/`int`/`long`.
     */
    @Query(
        nativeQuery = true,
        value = "SELECT CAST(pg_logical_emit_message(:tx, :prefix, :content) AS VARCHAR)",
    )
    fun emitLogicalMessage(
        @Param("tx") transactional: Boolean,
        @Param("prefix") prefix: String,
        @Param("content") content: String,
    ): String
}
