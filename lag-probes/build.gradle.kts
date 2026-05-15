// LagProbe adapters — Postgres queries `pg_replication_slots`,
// MySQL queries `SHOW MASTER STATUS`. Depends on both source
// modules because the MySQL probe needs `MySqlBinlogRowChangeSource.serverId`
// to derive the matching checkpoint key, and the Postgres probe
// reuses `ConnectionProvider`. Also depends on checkpoint-file so
// the scheduler can read the persisted MySQL position.
dependencies {
    api(project(":core"))
    api(project(":source-postgres"))
    api(project(":source-mysql"))
    api(project(":checkpoint-file"))

    testImplementation(project(":test-support"))
}
