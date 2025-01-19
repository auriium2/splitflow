# planner (idk how it works)
FROM rust:1.84 AS planner
RUN cargo install cargo-chef
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN cargo chef prepare --recipe-path recipe.json

# builder
FROM rust:1.84 AS builder
RUN cargo install cargo-chef
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN rm -f ./target/release/splitflow
RUN cargo build --release

# Third stage: Minimal runtime with Debian slim
FROM debian:bullseye-slim

# Install required runtime dependencies (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the build stage
COPY --from=builder /app/target/release/splitflow /usr/src/splitflow

# Ensure the binary is executable
RUN chmod +x /usr/src/splitflow

# Set the default command to run the binary
CMD ["/usr/src/splitflow"]