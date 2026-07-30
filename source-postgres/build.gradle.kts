// Postgres adapters — both `PgLogicalReplicationCdcSource` (legacy
// message-only path) and `PgWalRowChangeSource` (Wave 5.2 row-level)
// plus the shared `replication/` infrastructure (connector, config,
// wal2json parsers).
//
// `spring-context` is compileOnly so the parsers can carry a
// `@Component` annotation (consumed only when Spring is on the
// classpath) without forcing every consumer to drag Spring in.
dependencies {
    api(project(":core"))
    api("org.postgresql:postgresql:42.6.0")
    implementation("com.zaxxer:HikariCP:7.0.2")
    compileOnly("org.springframework:spring-context:6.0.13")

    testImplementation("org.springframework:spring-context:6.0.13")
    testImplementation(project(":test-support"))
}
