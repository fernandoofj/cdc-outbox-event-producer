package com.example.ordersapp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean

@SpringBootApplication
class OrdersSampleApplication {
    /**
     * Spring Boot 4's `spring-boot-starter-web` defaults to the new
     * Jackson 3 (`tools.jackson`) facade and doesn't auto-register a
     * classic `com.fasterxml.jackson.databind.ObjectMapper` bean. The
     * library itself (`ObjectMapperSingleton`) is still on classic
     * Jackson 2 — this bean keeps the sample app's payload
     * serialization on the same generation.
     */
    @Bean
    fun objectMapper(): ObjectMapper = jacksonObjectMapper()
}

fun main(args: Array<String>) {
    runApplication<OrdersSampleApplication>(*args)
}
