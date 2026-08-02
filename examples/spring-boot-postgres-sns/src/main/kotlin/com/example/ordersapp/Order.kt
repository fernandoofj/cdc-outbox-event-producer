package com.example.ordersapp

import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table

/**
 * `var` (not `val`) on every field: Hibernate sets [id] by reflection
 * after the IDENTITY insert, which requires a mutable backing field.
 */
@Entity
@Table(name = "orders")
class Order(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    var status: String = "",
    var totalCents: Long = 0,
)
