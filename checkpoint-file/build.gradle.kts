// File-backed CheckpointStore adapter. Zero external deps beyond
// core — the format is hand-rolled JSON to keep this leaf adapter
// from pulling Jackson into its compile classpath.
dependencies {
    api(project(":core"))
}
