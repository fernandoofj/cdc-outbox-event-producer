// Core module — domain + ports + application orchestrator. Zero
// framework deps: Kotlin stdlib + slf4j (logging interface) + Jackson
// (used by the mapping-rules default payload serializer) + Micrometer
// (metric facade — API only, no implementation chosen here).
// Everything else lives in an adapter. `org.json` moved to `:legacy`
// (Round 23) — JsonHelper was the only user and legacy's
// SlotReaderMessageProducer is the only caller; core no longer needs
// to expose a JSON driver it doesn't otherwise use.
dependencies {
    api("org.slf4j:slf4j-api:2.0.18")
    api("io.micrometer:micrometer-core:1.16.5")
    api("com.fasterxml.jackson.module:jackson-module-kotlin:2.22.1")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.22.1")
}
