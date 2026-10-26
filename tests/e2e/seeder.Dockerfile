# This repository's seedcrystal binary. Build context: the repository root.
FROM rust:1.98.0-trixie AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin seedcrystal \
    && install -m 0755 target/release/seedcrystal /usr/local/bin/seedcrystal

FROM debian:trixie-20261020-slim
# dig answers the container's readiness check.
RUN apt-get update \
    && apt-get install -y --no-install-recommends bind9-dnsutils \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/seedcrystal /usr/local/bin/seedcrystal
ENTRYPOINT ["seedcrystal"]
