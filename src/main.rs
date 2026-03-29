#[allow(dead_code)]
mod config;
#[allow(dead_code)] // TODO(task-8.2): remove when Orchestrator wires up SchemaInspector
mod discovery;
#[allow(dead_code)] // TODO(task-8.2): remove when Orchestrator wires up BatchExtractor
mod extractor;
#[allow(dead_code)] // TODO(task-8.2): remove when Orchestrator is implemented
mod orchestrator;
#[allow(dead_code)] // TODO(task-8.2): remove when Orchestrator wires up QueryBuilder
mod query;
#[allow(dead_code)] // TODO(task-8.2): remove when Orchestrator wires up StateManager
mod state;
#[allow(dead_code)] // TODO(task-7.2): remove when DeltaWriter is implemented
mod writer;

// TODO(task-9.2): install tokio::signal handler for SIGTERM/SIGINT
// TODO(task-10.2): initialize tracing_subscriber::fmt with structured output

fn main() {
    println!("Hello, world!");
}
