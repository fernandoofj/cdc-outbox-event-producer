package br.com.fltech.cdc.outbox.publisher.infra.spring

import br.com.fltech.cdc.outbox.publisher.core.port.CdcSource
import br.com.fltech.cdc.outbox.publisher.core.port.EventSinkRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.actuate.info.InfoContributor
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

/**
 * Wires the [CdcOutboxInfoContributor] that surfaces the producer's
 * non-sensitive configuration under `/actuator/info`.
 *
 * Kept in a dedicated auto-configuration (rather than folded into
 * [CdcOutboxHealthAutoConfiguration]) so a consumer on a class path
 * that ships `spring-boot-actuator` but does NOT expose Actuator
 * endpoints can still skip the contributor independently of the
 * health indicator.
 */
@AutoConfiguration
@AutoConfigureAfter(
    CdcOutboxAutoConfiguration::class,
    CdcOutboxHexagonalAutoConfiguration::class,
    CdcOutboxSinkAutoConfiguration::class,
)
@ConditionalOnClass(InfoContributor::class)
@ConditionalOnProperty(prefix = "cdc.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CdcOutboxProperties::class)
open class CdcOutboxInfoAutoConfiguration {

    /**
     * Both `EventSinkRegistry` and `CdcSource` are optional collaborators
     * — a misconfigured deploy may run without either, and the info
     * contributor must still report what little it can. Injected as
     * [ObjectProvider] so an absent bean does not block context
     * refresh.
     */
    @Bean
    @ConditionalOnMissingBean(CdcOutboxInfoContributor::class)
    open fun cdcOutboxInfoContributor(
        properties: CdcOutboxProperties,
        sinkRegistry: ObjectProvider<EventSinkRegistry>,
        source: ObjectProvider<CdcSource>,
    ): CdcOutboxInfoContributor = CdcOutboxInfoContributor(properties, sinkRegistry, source)
}
