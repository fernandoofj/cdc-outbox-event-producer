package br.com.fltech.outbox.publisher.infra.spring

import br.com.fltech.outbox.publisher.core.application.DefaultMappingRules
import br.com.fltech.outbox.publisher.core.port.MappingRules
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

/**
 * Builds a [MappingRules] bean from the YAML-bound
 * `cdc.outbox.mappings` list (Wave 3.5 / brief item 7).
 *
 * If the list is empty (the default), the wired `MappingRules` is
 * [MappingRules.EMPTY] — the orchestrator drops any unmapped row.
 *
 * The default payload serializer uses the application's Jackson
 * [ObjectMapper] when one is present on the context; otherwise falls
 * back to the deterministic `k=v` text form documented on
 * [DefaultMappingRules.DEFAULT_SERIALIZER]. Consumers can supply their
 * own [MappingRules] bean to override the whole chain.
 */
@AutoConfiguration
@AutoConfigureAfter(CdcOutboxAutoConfiguration::class)
@ConditionalOnProperty(prefix = "cdc.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CdcOutboxProperties::class)
open class CdcOutboxMappingAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    open fun cdcOutboxMappingRules(
        properties: CdcOutboxProperties,
        // `ObjectProvider` is the Spring-idiomatic way to express "an
        // optional dependency that might not be in the context". A bare
        // nullable parameter would still require the bean to be present;
        // `getIfAvailable()` returns null cleanly when it isn't, which
        // matches the documented Jackson-or-fallback contract.
        objectMapperProvider: ObjectProvider<ObjectMapper>,
    ): MappingRules {
        if (properties.mappings.isEmpty()) return MappingRules.EMPTY
        val tableMappings = properties.mappings.map { it.toDomain() }
        val objectMapper: ObjectMapper? = objectMapperProvider.getIfAvailable()
        val serializer: (Map<String, Any?>) -> ByteArray =
            if (objectMapper != null) {
                { payload -> objectMapper.writeValueAsBytes(payload) }
            } else {
                DefaultMappingRules.DEFAULT_SERIALIZER
            }
        return DefaultMappingRules(tableMappings, serializer)
    }
}
