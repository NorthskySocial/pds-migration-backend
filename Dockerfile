FROM rust:1.91.1 AS builder

# Copy local code to the container image.
WORKDIR /app

COPY Cargo.toml rust-toolchain.toml ./
COPY pdsmigration-common pdsmigration-common
COPY pdsmigration-gui pdsmigration-gui
COPY pdsmigration-web pdsmigration-web

RUN cargo build --release --package pdsmigration-web

FROM debian:trixie-slim

RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/pdsmigration-web /app/

ENTRYPOINT ["/app/pdsmigration-web"]

LABEL org.opencontainers.image.source=https://github.com/NorthskySocial/pds-migration
LABEL org.opencontainers.image.description="PDS migration tool"
LABEL org.opencontainers.image.licenses=MIT