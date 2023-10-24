package shop.inventa.pg2sns4k.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.messaging.converter.MappingJackson2MessageConverter

@Configuration
class MappingJackson2MessageConverterCustom(private val objectMapperWired: ObjectMapper) {
    @Bean
    fun jackson2MessageConverter() = MappingJackson2MessageConverter().apply {
        objectMapper = objectMapperWired
        objectMapper.registerModule(JavaTimeModule())
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        serializedPayloadClass = String::class.java
    }
}
