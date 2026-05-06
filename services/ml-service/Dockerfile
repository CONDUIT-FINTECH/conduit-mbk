# ═══════════════════════════════════════════════════════════════
#  Conduit ML Service — Dockerfile
#  Multi-stage build: smaller final image, no build tools at runtime.
# ═══════════════════════════════════════════════════════════════

# ── Stage 1: dependency installation ────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System libs required by confluent-kafka (librdkafka) and scipy
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime image ───────────────────────────────────────────
FROM python:3.12-slim

# Runtime system deps (confluent-kafka needs librdkafka at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy service source
COPY src ./src
COPY pyproject.toml .

# Non-root user for security
RUN useradd -m -u 1001 conduit
USER conduit

EXPOSE 4008

# K8s / Docker health check (matches /ready semantics)
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:4008/ready || exit 1

CMD ["python", "-m", "src.main"]
