# mssql-pg-migrate Docker image
# Downloads the binary from GitHub releases

FROM debian:bookworm-slim

ARG VERSION=1.24.0
ARG TARGETARCH=amd64

# Install ca-certificates for HTTPS connections to databases
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download binary from GitHub releases
RUN curl -fsSL "https://github.com/johndauphine/mssql-pg-migrate/releases/download/v${VERSION}/mssql-pg-migrate-linux-${TARGETARCH}" \
    -o /usr/local/bin/mssql-pg-migrate \
    && chmod +x /usr/local/bin/mssql-pg-migrate

# Create non-root user
RUN useradd -m -s /bin/bash migrate
USER migrate
WORKDIR /home/migrate

# Default entrypoint
ENTRYPOINT ["mssql-pg-migrate"]
CMD ["--help"]
