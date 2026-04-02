FROM rust:1.94.0-bookworm AS chef
RUN cargo install cargo-chef --version 0.1.77
WORKDIR /src

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p contextdb-server
COPY . .
RUN cargo build --release -p contextdb-server \
    && strip target/release/contextdb-server

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/contextdb-server /usr/local/bin/contextdb-server

EXPOSE 4222

ENTRYPOINT ["contextdb-server"]
