// Sink composition primitives: CompositeEventSink (fan-out),
// SchemeRouterEventSink (delegates by scheme), DefaultEventSinkRegistry
// (the EventSinkRegistry implementation Spring auto-wires). No broker
// deps — all routing is over the EventSink port.
dependencies {
    api(project(":core"))
}
