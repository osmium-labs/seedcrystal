# Unmodified Raptoreum Core from the official release tarball.
FROM ubuntu:jammy-20260930

ARG RAPTOREUM_VERSION=2.0.3.01-mainnet
# The release publishes only SHA-1 and SHA-256 sums (checksums.txt), no SHA-512.
# SHA-256 below is the published value for this tarball; the SHA-512 was computed
# from the downloaded file after that SHA-256 matched.
ARG RAPTOREUM_SHA256=1dfb7b127485b668eb9cbeecd585028075580aaa302d96dad89f9790741aeb60
ARG RAPTOREUM_SHA512=bc89f6567699bca8ea89b9155d8681595018d01c3b42cae3b89ce7543b81746d1c86535bc389b3b1ddbbce2b291ffb1919f051164dc5d86b380fd91fd33337ad

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    tarball="raptoreum-ubuntu22-${RAPTOREUM_VERSION}-.tar.gz"; \
    curl -fsSL -o "/tmp/${tarball}" \
      "https://github.com/Raptor3um/raptoreum/releases/download/${RAPTOREUM_VERSION}/${tarball}"; \
    echo "${RAPTOREUM_SHA256}  /tmp/${tarball}" | sha256sum -c -; \
    echo "${RAPTOREUM_SHA512}  /tmp/${tarball}" | sha512sum -c -; \
    mkdir /tmp/raptoreum; \
    tar -xzf "/tmp/${tarball}" -C /tmp/raptoreum; \
    install -m 0755 /tmp/raptoreum/raptoreumd /tmp/raptoreum/raptoreum-cli /usr/local/bin/; \
    rm -rf /tmp/raptoreum "/tmp/${tarball}"; \
    apt-get purge -y --auto-remove curl; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir /data; \
    (ldd /usr/local/bin/raptoreumd | grep 'not found' && exit 1 || true); \
    raptoreumd -version | head -1

EXPOSE 10230
ENTRYPOINT ["raptoreumd", "-datadir=/data"]
