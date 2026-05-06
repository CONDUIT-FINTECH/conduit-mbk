"""
Conduit ML Service — Entry Point
==================================
Starts two concurrent components:

1. **Kafka consumer thread** — polls ``conduit.metrics.computed`` and
   ``conduit.events.ingested``, runs anomaly detection, and publishes
   predictions to ``conduit.ml.predictions``.

2. **FastAPI / Uvicorn HTTP server** — serves ``/health``, ``/ready``,
   ``/metrics``, and ``/models`` on ``ML_PORT`` (default 4008).

Boot sequence::

    1. Load configuration from environment variables
    2. Construct EventProcessor (holds DetectorRegistry + MLProducer)
    3. Construct MLConsumer (wired to processor.handle_message)
    4. Start Kafka thread (daemon=True — exits with the main process)
    5. Start Uvicorn (blocks until SIGTERM / SIGINT)
    6. Graceful shutdown: stop consumer, flush producer

Running locally::

    cd services/ml-service
    pip install -r requirements.txt
    cp .env.example .env       # edit as needed
    python -m src.main

Or with the dev server (auto-reload on code changes)::

    uvicorn src.main:app --host 0.0.0.0 --port 4008 --reload

Environment variables: see ``.env.example`` and ``src/config.py``.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
from typing import Any, Dict

from dotenv import load_dotenv  # type: ignore[import-untyped]

# Load .env before importing config so env vars are available
load_dotenv()

from src.api.health import build_app
from src.config import config
from src.kafka.consumer import MLConsumer
from src.pipeline.processor import EventProcessor

# ─── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, config.server.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("conduit.ml-service")

# ─── Global state ─────────────────────────────────────────────────────
_processor: EventProcessor | None = None
_consumer: MLConsumer | None = None
_ready = False


# ─────────────────────────────────────────────────────────────────────
#  FastAPI app (module-level so ``uvicorn src.main:app`` works)
# ─────────────────────────────────────────────────────────────────────

def _get_processor_stats() -> Dict[str, Any]:
    return _processor.stats if _processor else {}


def _get_model_stats() -> Dict[str, Any]:
    return _processor.model_stats if _processor else {}


def _is_ready() -> bool:
    return _ready or (config.kafka.dry_run)


app = build_app(
    processor_stats_fn=_get_processor_stats,
    model_stats_fn=_get_model_stats,
    is_ready_fn=_is_ready,
)


# ─────────────────────────────────────────────────────────────────────
#  Lifecycle
# ─────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup() -> None:
    """Called by Uvicorn after the server is ready to accept connections."""
    global _processor, _consumer, _ready

    logger.info("[ML Service] Booting...")
    logger.info("[ML Service] Kafka brokers : %s", config.kafka.broker_str)
    logger.info("[ML Service] Topics (in)   : %s", config.kafka.topics_consume)
    logger.info("[ML Service] Topic (out)   : %s", config.kafka.topic_predictions)
    logger.info("[ML Service] Dry-run       : %s", config.kafka.dry_run)

    # 1. Build processor (holds model registry + producer)
    _processor = EventProcessor(config)

    # Mark ready immediately in dry-run so /ready returns 200
    if config.kafka.dry_run:
        _ready = True
        logger.info("[ML Service] Ready (dry-run mode)")
        return

    # 2. Build consumer
    _consumer = MLConsumer(
        config.kafka,
        handler=_on_message,
    )

    # 3. Start Kafka poll loop in a background daemon thread
    t = threading.Thread(target=_consumer.start, name="kafka-consumer", daemon=True)
    t.start()

    _ready = True
    logger.info("[ML Service] Ready — Kafka consumer running on background thread")
    logger.info(
        "[ML Service] Serving HTTP on %s:%d", config.server.host, config.server.port
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    """Called by Uvicorn on SIGTERM / SIGINT."""
    global _ready
    _ready = False
    logger.info("[ML Service] Shutdown initiated...")

    if _consumer:
        _consumer.stop()

    if _processor:
        _processor.flush()

    logger.info("[ML Service] Shutdown complete")


# ─────────────────────────────────────────────────────────────────────
#  Message handler glue (with ready-gate)
# ─────────────────────────────────────────────────────────────────────

def _on_message(value: Dict[str, Any], topic: str) -> None:
    """
    Thin wrapper around ``EventProcessor.handle_message`` that:

    * Marks the service ready after the first successfully processed message
    * Propagates exceptions so the consumer can route them to the DLQ
    """
    if _processor is None:
        return
    _processor.handle_message(value, topic)


# ─────────────────────────────────────────────────────────────────────
#  Direct execution  (python -m src.main)
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import uvicorn  # type: ignore[import-untyped]

    uvicorn.run(
        "src.main:app",
        host=config.server.host,
        port=config.server.port,
        log_level=config.server.log_level,
        # Disable uvicorn's own access log so our structured logger is the
        # single source of truth
        access_log=False,
    )


if __name__ == "__main__":
    main()
