# Multi-stage build — recon-agent
# Targets linux/arm64 (OCI A1.Flex) and linux/amd64 (CI / x86 dev)
#
# Stages:
#   builder  — installs all deps into a venv
#   runtime  — copies the venv + src into a slim image

# ── Stage 1: builder ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System deps needed to compile some Python packages (asyncpg uses Cython)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create an isolated venv so stage 2 can copy it cleanly
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install project + kafka extras (aiokafka, watchdog)
# Copy only the packaging metadata first so this layer is cached on code changes
COPY pyproject.toml ./
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -e ".[kafka,agent]"

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Runtime system deps (libpq for asyncpg)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy the pre-built venv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Non-root user for security
RUN useradd --system --no-create-home --uid 1001 recon
WORKDIR /app

# Copy application source
COPY src/ ./src/
COPY migrations/ ./migrations/
COPY scripts/ ./scripts/

# Make sure the package is importable from /app/src
ENV PYTHONPATH="/app/src"
ENV PYTHONUNBUFFERED="1"
ENV PYTHONDONTWRITEBYTECODE="1"

USER recon

# Default entrypoint — override with `command:` in docker-compose.yaml
ENTRYPOINT ["python", "-m", "recon.cli"]
CMD ["--help"]
