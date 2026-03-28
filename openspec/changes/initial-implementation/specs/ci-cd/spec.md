## ADDED Requirements

### Requirement: CI pipeline runs on every push and pull request
The system SHALL have a GitHub Actions CI workflow triggered on push to any branch and on pull requests to main. The workflow SHALL run `cargo test`, `cargo clippy -- -D warnings`, and `cargo llvm-cov --fail-under-lines 90` sequentially. Any failure SHALL block the PR from merging.

#### Scenario: All checks pass
- **WHEN** a PR is opened and all tests pass, clippy has no warnings, and coverage is >=90%
- **THEN** the CI workflow SHALL report success and the PR is mergeable

#### Scenario: Test failure
- **WHEN** a PR introduces a failing test
- **THEN** the CI workflow SHALL fail at the `cargo test` step and block the PR

#### Scenario: Clippy warning
- **WHEN** code produces a clippy warning
- **THEN** the CI workflow SHALL fail (clippy runs with `-D warnings`) and block the PR

#### Scenario: Coverage below 90%
- **WHEN** code changes cause line coverage to drop below 90%
- **THEN** the CI workflow SHALL fail at the `cargo llvm-cov --fail-under-lines 90` step and block the PR

### Requirement: Coverage report uploaded as CI artifact
The CI workflow SHALL upload the llvm-cov coverage report as a GitHub Actions artifact for review, regardless of pass/fail status.

#### Scenario: CI run completes
- **WHEN** the CI workflow finishes (pass or fail)
- **THEN** the coverage report SHALL be available as a downloadable artifact named `coverage-report`

### Requirement: Binary release on version tags
The system SHALL have a GitHub Actions release workflow triggered on tags matching `v*` (e.g., `v0.1.0`). The workflow SHALL cross-compile the binary for 4 targets: `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`. All binaries SHALL be published to GitHub Releases with SHA256 checksums.

#### Scenario: Version tag pushed
- **WHEN** tag `v0.1.0` is pushed
- **THEN** the workflow SHALL build 4 binaries, generate checksums, and create a GitHub Release with all artifacts attached

#### Scenario: Cross-compilation for musl targets
- **WHEN** building for `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`
- **THEN** the workflow SHALL use the `cross` tool to produce fully static binaries

#### Scenario: Cross-compilation for gnu targets
- **WHEN** building for `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`
- **THEN** the workflow SHALL use the `cross` tool with appropriate sysroot linking

### Requirement: Container image published to GHCR
The release workflow SHALL build a multi-arch Docker image (`linux/amd64`, `linux/arm64`) based on `alpine:latest` and push it to `ghcr.io/<OWNER>/parket` with tags `latest` and the version (e.g., `0.1.0`).

#### Scenario: Version tag pushed
- **WHEN** tag `v0.1.0` is pushed
- **THEN** the workflow SHALL push `ghcr.io/<OWNER>/parket:latest` and `ghcr.io/<OWNER>/parket:0.1.0` with both `linux/amd64` and `linux/arm64` manifests

#### Scenario: Container image is minimal
- **WHEN** the Docker image is built
- **THEN** the image SHALL be based on `alpine:latest` containing only the compiled `parket` binary and no build tooling

### Requirement: Dockerfile uses multi-stage build
The project SHALL include a Dockerfile using a multi-stage build: first stage compiles the binary using `rust:alpine` with musl target, second stage copies the binary into `alpine:latest` runtime image.

#### Scenario: Docker build succeeds
- **WHEN** `docker build .` is executed
- **THEN** the build SHALL produce a minimal Alpine image containing the `parket` binary at `/usr/local/bin/parket`

### Requirement: CI uses pinned Rust toolchain
The CI workflow SHALL use a pinned Rust toolchain version specified in a `rust-toolchain.toml` file at the project root. This ensures reproducible builds across local development and CI.

#### Scenario: CI runs with pinned toolchain
- **WHEN** the CI workflow starts
- **THEN** it SHALL install the exact Rust version from `rust-toolchain.toml` and the `llvm-tools-preview` component for coverage
