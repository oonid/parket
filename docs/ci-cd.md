# CI/CD

Parket uses two GitHub Actions workflows: **CI** for continuous quality checks and **Release** for publishing binaries and container images.

## CI Workflow

**File:** `.github/workflows/ci.yml`

**Trigger:** Every push to any branch, every PR targeting `main`.

**Pipeline steps:**

| Step | Command | Purpose |
|---|---|---|
| Install toolchain | `dtolnay/rust-toolchain@stable` + clippy + llvm-tools-preview | Match `rust-toolchain.toml`, install coverage tools |
| Cache | `actions/cache@v4` on `~/.cargo/registry`, `~/.cargo/git`, `target` | Speed up builds via Cargo.lock-based cache key |
| Build | `cargo build` | Compile all targets |
| Test | `cargo test --lib` | Run unit tests |
| Clippy | `cargo clippy -- -D warnings` | Zero-tolerance lint gate |
| Coverage | `cargo llvm-cov --fail-under-lines 90` | Hard gate: CI fails below 90% line coverage |
| Upload report | `actions/upload-artifact@v4` | LCOV report retained 30 days |

**Coverage gate:** The 90% line coverage threshold is a hard gate. CI fails if coverage drops below this. Reports are uploaded as artifacts for review.

## Release Workflow

**File:** `.github/workflows/release.yml`

**Trigger:** Push of a version tag (`v*`, e.g. `v0.1.0`).

### Jobs

The release workflow runs three jobs: `build` and `docker` run in parallel; `release` runs after `build` completes.

#### 1. Build (cross-compiled binaries)

Uses `cross` to build 4 binary targets in parallel via matrix strategy:

| Target | Variant | Use case |
|---|---|---|
| `x86_64-unknown-linux-musl` | Static (musl) | Most Linux servers, Alpine containers |
| `aarch64-unknown-linux-musl` | Static (musl) | ARM servers, ARM Alpine |
| `x86_64-unknown-linux-gnu` | Dynamic (glibc) | Standard Linux (glibc) |
| `aarch64-unknown-linux-gnu` | Dynamic (glibc) | ARM Linux (glibc) |

Each binary is renamed to `parket-<target>` and uploaded as a build artifact.

#### 2. Release (GitHub Release)

Runs after `build`. Downloads all binaries, generates SHA256 checksums, and creates a GitHub Release with auto-generated release notes.

**Release artifacts:**
- `parket-x86_64-unknown-linux-musl`
- `parket-aarch64-unknown-linux-musl`
- `parket-x86_64-unknown-linux-gnu`
- `parket-aarch64-unknown-linux-gnu`
- `checksums-sha256.txt`

#### 3. Docker (GHCR container image)

Builds a multi-arch Docker image and pushes to GitHub Container Registry.

**Image tags:**
- `ghcr.io/oonid/parket:latest`
- `ghcr.io/oonid/parket:<version>` (extracted from tag, e.g. `0.1.0`)

**Platforms:** `linux/amd64`, `linux/arm64`

**Features:**
- QEMU + Docker Buildx for cross-platform builds
- GitHub Actions cache (`type=gha`) for Docker layer caching
- Uses `GITHUB_TOKEN` for authentication (no secrets needed)

### Release Process

To create a release:

```bash
git tag v0.1.0
git push origin v0.1.0
```

This triggers the release workflow which:
1. Builds 4 cross-compiled binaries
2. Creates a GitHub Release with binaries + checksums
3. Builds and pushes a multi-arch Docker image to GHCR

## Docker

### Dockerfile

**File:** `Dockerfile`

Multi-stage build for minimal image size:

**Builder stage** (`rust:1.92-alpine`):
- Installs build dependencies: `build-base`, `pkgconf`, `openssl-dev`, `openssl-libs-static`, `zlib-dev`, `zlib-static`, `mariadb-connector-c-dev`
- Uses dependency caching: builds a dummy binary first to cache crate compilation, then rebuilds with actual source
- Produces a static musl binary

**Runtime stage** (`alpine:latest`):
- Installs only runtime dependencies: `ca-certificates`, `mariadb-connector-c`
- Copies the release binary to `/usr/local/bin/parket`
- Sets `ENTRYPOINT ["parket"]`

### Local Docker Build

```bash
docker build -t parket .
```

### Running with Docker

```bash
docker run --rm \
  -e DATABASE_URL=mysql://user:pass@host:3306/dbname \
  -e S3_BUCKET=data-lake \
  -e S3_ACCESS_KEY_ID=minioadmin \
  -e S3_SECRET_ACCESS_KEY=minioadmin \
  -e S3_ENDPOINT=http://minio:9000 \
  -e TABLES=orders,customers \
  -e TARGET_MEMORY_MB=512 \
  ghcr.io/oonid/parket:latest
```

### Running with Docker Compose (with MinIO)

```yaml
version: "3"
services:
  parket:
    image: ghcr.io/oonid/parket:latest
    environment:
      DATABASE_URL: mysql://user:pass@mariadb:3306/dbname
      S3_BUCKET: data-lake
      S3_ACCESS_KEY_ID: minioadmin
      S3_SECRET_ACCESS_KEY: minioadmin
      S3_ENDPOINT: http://minio:9000
      S3_REGION: us-east-1
      TABLES: orders,customers
      TARGET_MEMORY_MB: 512
```

## Downloading Binaries

Download from [GitHub Releases](https://github.com/oonid/parket/releases):

```bash
# Download and verify
curl -LO https://github.com/oonid/parket/releases/latest/download/parket-x86_64-unknown-linux-musl
chmod +x parket-x86_64-unknown-linux-musl

# Verify checksum
sha256sum -c checksums-sha256.txt --ignore-missing
```
