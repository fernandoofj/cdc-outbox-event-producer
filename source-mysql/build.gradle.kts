// MySQL adapters — `MySqlOutboxTableCdcSource` (poller) and
// `MySqlBinlogRowChangeSource` (binlog ROW format). Both connector
// libs are `compileOnly` so consumers that only use one don't pay
// for both transitively; tests pull them in directly.
dependencies {
    api(project(":core"))
    implementation("com.zaxxer:HikariCP:7.0.2")
    compileOnly("com.mysql:mysql-connector-j:8.4.0")
    compileOnly("com.zendesk:mysql-binlog-connector-java:0.29.2")

    testImplementation("com.mysql:mysql-connector-j:8.4.0")
    testImplementation("com.zendesk:mysql-binlog-connector-java:0.29.2")
    testImplementation(project(":test-support"))
}
