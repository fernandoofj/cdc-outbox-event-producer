// Core module — domain + ports + application orchestrator. Zero
// framework deps: Kotlin stdlib + slf4j (logging interface) + Jackson
// (for the helpers/JsonHelper utility that mapping rules use) +
// Micrometer (metric facade — API only, no implementation chosen
// here). Everything else lives in an adapter.
dependencies {
    api("org.slf4j:slf4j-api:2.0.18")
    api("io.micrometer:micrometer-core:1.16.5")
    api("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.3")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.3")
    api("org.json:json:20240205")
}
