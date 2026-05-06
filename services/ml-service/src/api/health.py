"""
FastAPI health & management endpoints
======================================
Provides the same endpoint conventions used by the Node.js services:

``GET /health``
    Liveness probe — always returns 200 while the process is alive.
    Includes pipeline stats and Kafka consumer status.

``GET /ready``
    Readiness probe — returns 200 once the Kafka consumer has started
    and the first message has been received (or dry-run mode is active).
    Returns 503 while warming up.

``GET /metrics``
    Lightweight JSON metrics for monitoring (total messages, predictions,
    errors, active tenants).

``GET /models``
    Per-tenant model diagnostics (sample count, anomaly count, warm-up
    status, window size).

Usage (wired up in ``main.py``)::

    from src.api.health import build_app
    app = build_app(processor, consumer_status)
"""

from __future__ import annotations

import time
from typing import Callable, Dict, Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse


_start_time = time.time()


def build_app(
    processor_stats_fn: Callable[[], Dict[str, Any]],
    model_stats_fn: Callable[[], Dict[str, Any]],
    is_ready_fn: Callable[[], bool],
    service_version: str = "1.0.0",
) -> FastAPI:
    """
    Construct and return the FastAPI application.

    Parameters
    ----------
    processor_stats_fn:
        Zero-argument callable that returns the current pipeline stats
        dict (from ``EventProcessor.stats``).
    model_stats_fn:
        Zero-argument callable that returns per-tenant model stats
        (from ``EventProcessor.model_stats``).
    is_ready_fn:
        Zero-argument callable that returns ``True`` once the service is
        ready to serve predictions.
    service_version:
        Embedded in the health response for traceability.
    """
    app = FastAPI(
        title="Conduit ML Service",
        description=(
            "Anomaly-detection microservice for the Conduit platform. "
            "Consumes Kafka metric snapshots and publishes ML predictions."
        ),
        version=service_version,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ─────────────────────────────────────────────────────────────────
    #  GET /health  (liveness)
    # ─────────────────────────────────────────────────────────────────

    @app.get("/health", summary="Liveness probe")
    def health() -> JSONResponse:
        """
        Returns 200 while the process is alive.

        The ``pipeline`` block mirrors the structure used by the Node.js
        Metrics Service health endpoint so dashboard tooling can parse it
        uniformly.
        """
        stats = processor_stats_fn()
        uptime = round(time.time() - _start_time, 1)

        return JSONResponse(
            status_code=200,
            content={
                "service": "ml-service",
                "version": service_version,
                "status": "healthy",
                "uptime": uptime,
                "pipeline": stats,
                "timestamp": _now_iso(),
            },
        )

    # ─────────────────────────────────────────────────────────────────
    #  GET /ready  (readiness)
    # ─────────────────────────────────────────────────────────────────

    @app.get("/ready", summary="Readiness probe")
    def ready() -> JSONResponse:
        """
        Returns 200 once the Kafka consumer is running and the first
        message has been processed (or in dry-run mode).

        K8s / Docker health-check usage::

            HEALTHCHECK --interval=10s --timeout=5s --retries=3 \\
              CMD curl -f http://localhost:4008/ready || exit 1
        """
        ready_status = is_ready_fn()
        status_code = 200 if ready_status else 503
        return JSONResponse(
            status_code=status_code,
            content={"status": "ready" if ready_status else "not_ready"},
        )

    # ─────────────────────────────────────────────────────────────────
    #  GET /metrics  (operational metrics)
    # ─────────────────────────────────────────────────────────────────

    @app.get("/metrics", summary="Pipeline metrics")
    def metrics() -> JSONResponse:
        """
        Lightweight JSON metrics suitable for Prometheus scraping or
        manual inspection.
        """
        stats = processor_stats_fn()
        return JSONResponse(
            status_code=200,
            content={
                **stats,
                "uptime": round(time.time() - _start_time, 1),
                "timestamp": _now_iso(),
            },
        )

    # ─────────────────────────────────────────────────────────────────
    #  GET /models  (model registry)
    # ─────────────────────────────────────────────────────────────────

    @app.get("/models", summary="Per-tenant model diagnostics")
    def models() -> JSONResponse:
        """
        Returns per-tenant anomaly detector state.

        Useful for debugging: shows how many samples each tenant has
        accumulated, whether the IsolationForest is trained, and the
        anomaly rate observed so far.
        """
        model_stats = model_stats_fn()
        return JSONResponse(
            status_code=200,
            content={
                "tenants": model_stats,
                "tenantCount": len(model_stats),
                "timestamp": _now_iso(),
            },
        )

    return app


# ─── Helper ──────────────────────────────────────────────────────────

def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
