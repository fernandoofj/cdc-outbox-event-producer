// Placeholder source adapters for Oracle and SQL Server. They throw
// `UnsupportedOperationException` on every `CdcSource` call so that
// a misconfigured deployment fails fast instead of silently emitting
// nothing. Real implementations will land in dedicated modules.
dependencies {
    api(project(":core"))
}
