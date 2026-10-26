# Unmodified Osmium Core from the official release tarball.
FROM debian:trixie-20261020-slim

ARG OSMIUM_VERSION=1.2.0
# SHA-512 of osmiumcore-1.2.0-x86_64-linux-gnu.tar.gz, from the release's checksums.txt.
ARG OSMIUM_SHA512=b902336aeb1ec539e5feb431a329d30e7042df2aaa7a18185d74e682e2bfb74d3642e05a0f80912bf9b93cad44e7764ba291dcdc1d56d5e5fdf13438c94b29e1

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    tarball="osmiumcore-${OSMIUM_VERSION}-x86_64-linux-gnu.tar.gz"; \
    curl -fsSL -o "/tmp/${tarball}" \
      "https://github.com/osmium-labs/osmium/releases/download/v${OSMIUM_VERSION}/${tarball}"; \
    echo "${OSMIUM_SHA512}  /tmp/${tarball}" | sha512sum -c -; \
    mkdir /tmp/osmium; \
    tar -xzf "/tmp/${tarball}" -C /tmp/osmium; \
    find /tmp/osmium -type f \( -name osmiumd -o -name osmium-cli \) -exec install -m 0755 {} /usr/local/bin/ \; ; \
    rm -rf /tmp/osmium "/tmp/${tarball}"; \
    apt-get purge -y --auto-remove curl; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir /data; \
    osmiumd -version

ENTRYPOINT ["osmiumd"]
