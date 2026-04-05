# PDS Migration Toolkit
[![License](https://img.shields.io/badge/license-MIT-blue)](https://opensource.org/licenses/mit)

A comprehensive toolkit for migrating Personal Data Servers (PDS) in the AT Protocol/Bluesky
ecosystem. This project provides both a desktop GUI application and a web service API for seamless
PDS migration operations.

## Overview

This toolkit enables users to migrate their data between Personal Data Servers in the AT Protocol
network. It consists of three main packages:

- **`pdsmigration-common`**: Core library containing migration logic, cryptographic utilities, and
  shared API functions
- **`pdsmigration-gui`**: Cross-platform desktop GUI application built with egui/eframe
- **`pdsmigration-web`**: HTTP web service built with Actix-Web, featuring AWS S3 integration and
  Prometheus metrics

## Technology Stack

- **Language**: Rust (1.91.1 toolchain required)
- **GUI Framework**: egui/eframe with both glow and wgpu rendering backends
- **Web Framework**: Actix-Web
- **Cloud Storage**: AWS S3 SDK
- **Cryptography**: secp256k1, multibase encoding
- **Data Formats**: IPLD, CBOR, JSON
- **Metrics**: Prometheus
- **Testing**: Standard Rust testing with mockall, wiremock

## Requirements

### System Requirements

- **Rust Toolchain**: 1.91.1 channel with `rustfmt` and `clippy` components
- **Platform Support**:
    - Native: Windows, macOS, Linux

### Installation

```bash
# Install Rust 1.91.1 (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install 1.91.1
rustup component add rustfmt clippy
```

## Setup & Running

### Quick Start

```bash
# Clone the repository
git clone https://github.com/NorthskySocial/pds-migration.git
cd pds-migration

# Build all packages
cargo build --release

# Run GUI application
cargo run -p pdsmigration-gui

# Run web service (requires environment setup)
cargo run -p pdsmigration-web
```

### Package-Specific Commands

#### Desktop GUI Application

```bash
# Build and run native GUI
cargo run -p pdsmigration-gui

# Release build
cargo build -p pdsmigration-gui --release
```

#### Web Service

```bash
# Run web service (development)
cargo run -p pdsmigration-web

# Build for production
cargo build -p pdsmigration-web --release

# Docker deployment
docker build -t pdsmigration .
docker run -p 9090:9090 --env-file .env pdsmigration
```

#### Common Library

```bash
# Build library
cargo build -p pdsmigration-common

# Run library tests
cargo test -p pdsmigration-common
```

## Environment Variables

### Web Service Configuration

| Variable        | Required | Default                 | Description                        |
|-----------------|----------|-------------------------|------------------------------------|
| `ENDPOINT`      | **Yes**  | -                       | S3-compatible storage endpoint URL |
| `PLC_DIRECTORY` | No       | `https://plc.directory` | PLC directory service URL          |
| `SERVER_PORT`   | No       | `9090`                  | HTTP server port                   |
| `WORKER_COUNT`  | No       | `2`                     | Number of worker threads           |

### AWS S3 Configuration

Standard AWS SDK environment variables are supported:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`

### Example .env file

```env
ENDPOINT=https://s3.amazonaws.com
SERVER_PORT=9090
WORKER_COUNT=4
PLC_DIRECTORY=https://plc.directory
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
```

## API Endpoints

The web service provides the following HTTP POST endpoints for migration:

1. `/service-auth` - Service authentication
2. `/create-account` - Create new account on target PDS
3. `/export-repo` - Export repository data
4. `/import-repo` - Import repository data
5. `/export-blobs` - Export blob data
6. `/upload-blobs` - Upload blobs to target PDS
7. `/migrate-preferences` - Migrate user preferences
8. `/request-token` - Request authentication token
9. `/migrate-plc` - Migrate PLC (Personal Data License)
10. `/activate-account` - Activate migrated account
11. `/deactivate-account` - Deactivate old account

Additional endpoints:

- `/health` - Health check endpoint
- `/metrics` - Prometheus metrics

## Testing

### Running Tests

```bash
# Run all tests
cargo test --workspace

# Run tests for specific package
cargo test -p pdsmigration-common
cargo test -p pdsmigration-gui
cargo test -p pdsmigration-web

# Run tests with output
cargo test --workspace -- --nocapture
```

## Development

### Code Quality

- All code must pass `cargo fmt --check`
- All code must pass `cargo clippy`
- Warnings are treated as errors in CI
- Use `#[tracing::instrument]` for debugging critical functions

### Build Targets

- **Native**: Standard desktop platforms
- **Docker**: Production containerized deployment

### CI/CD

GitHub Actions automatically:

- Runs comprehensive tests
- Checks code formatting and linting
- Builds Docker containers
- Publishes to GitHub Container Registry

## Contributing

1. Ensure you have Rust 1.91.1 installed
2. Format code: `cargo fmt`
3. Run tests: `cargo test --workspace`
4. Check linting: `cargo clippy`
5. Build all targets: `cargo build --workspace`