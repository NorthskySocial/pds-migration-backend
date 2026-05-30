FROM rust:1.95.0-slim AS chef
WORKDIR /app
RUN apt-get update \
 && apt-get install --no-install-recommends -y pkg-config libssl-dev curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY rust-toolchain.toml ./
RUN cargo install cargo-chef --locked --version ^0.1

FROM chef AS planner
COPY . .
# we don't need pdsmigration-gui's dependencies on our plan
RUN rm -rf pdsmigration-gui \
 && mkdir -p pdsmigration-gui/src \
 && printf '[package]\nname = "pdsmigration-gui"\nversion = "0.0.0"\nedition = "2021"\npublish = false\n\n[lib]\npath = "src/lib.rs"\n' > pdsmigration-gui/Cargo.toml \
 && : > pdsmigration-gui/src/lib.rs
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# re-stubbing pdsmigration-gui
RUN mkdir -p pdsmigration-gui/src \
 && printf '[package]\nname = "pdsmigration-gui"\nversion = "0.0.0"\nedition = "2021"\npublish = false\n\n[lib]\npath = "src/lib.rs"\n' > pdsmigration-gui/Cargo.toml \
 && : > pdsmigration-gui/src/lib.rs
RUN cargo chef cook --profile release-docker --recipe-path recipe.json -p pdsmigration-web

COPY Cargo.toml Cargo.lock* ./
COPY pdsmigration-common pdsmigration-common
COPY pdsmigration-web pdsmigration-web
RUN cargo build --profile release-docker --package pdsmigration-web

FROM debian:trixie-slim AS runtime
RUN apt-get update \
 && apt-get install --no-install-recommends -y ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release-docker/pdsmigration-web /app/

ENTRYPOINT ["/app/pdsmigration-web"]

LABEL org.opencontainers.image.source=https://github.com/NorthskySocial/pds-migration
LABEL org.opencontainers.image.description="PDS migration tool"
LABEL org.opencontainers.image.licenses=MIT