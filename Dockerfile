FROM rust:1.92-alpine AS builder

RUN apk add --no-cache build-base pkgconf openssl-dev openssl-libs-static zlib-dev zlib-static mariadb-connector-c-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM alpine:latest

RUN apk add --no-cache ca-certificates mariadb-connector-c

COPY --from=builder /app/target/release/parket /usr/local/bin/parket

ENTRYPOINT ["parket"]
