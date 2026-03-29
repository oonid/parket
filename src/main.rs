#[allow(dead_code)]
mod config;
#[allow(dead_code)] // TODO(task-5): remove when wired up in orchestrator
mod discovery;
#[allow(dead_code)] // TODO(task-6): remove when implemented
mod extractor;
#[allow(dead_code)] // TODO(task-8): remove when implemented
mod orchestrator;
#[allow(dead_code)] // TODO(task-4): remove when wired up in orchestrator
mod query;
#[allow(dead_code)] // TODO(task-3): remove when wired up in orchestrator
mod state;
#[allow(dead_code)] // TODO(task-7): remove when implemented
mod writer;

// TODO(task-9): signal handler (SIGTERM/SIGINT) in main
// TODO(task-10): structured logging with tracing-subscriber

fn main() {
    println!("Hello, world!");
}
