//! Crate root for integration tests.
//!
//! This project is a binary (`parket`), not a library. However, Rust integration tests
//! in `tests/` can only access the crate through its library target. Without `lib.rs`,
//! integration tests would have no way to import `Orchestrator`, `Config`, `DeltaWriter`,
//! or any other type — they cannot reach into `main.rs`.
//!
//! The pattern is:
//! - `lib.rs` declares the public module tree (single source of truth).
//! - `main.rs` imports from `parket::*` (same as integration tests).
//! - Integration tests in `tests/integration.rs` import via `use parket::...`.
//!
//! No public API stability is implied — this is purely an internal structuring choice.

pub mod config;
pub mod discovery;
pub mod extractor;
pub mod orchestrator;
pub mod query;
pub mod state;
pub mod writer;
