```python
import asyncio
import json
import logging
import os
import random
import sys
import time
import traceback as tb_module
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from typing import List
import httpx

# =============================================================================
# Checkout Service v2.2.0
#
# Orchestrates checkout flow:  User → Inventory → Payment → Notification
# All calls are SEQUENTIAL (synchronous within the request path).
#
# v2.2.0 fixes:
#   - Handle httpx.ConnectError explicitly in downstream call helpers
#   - Add circuit breaker for inventory-service and payment-service
#   - Use asyncio.sleep instead of blocking time.sleep for notification retries
#   - Reduce default INVENTORY_TIMEOUT from 8s to 5s for faster failure
# =============================================================================

SERVICE_NAME = "checkout-service"
SERVICE_VERSION = "2.2.0"

# ---------------------------------------------------------------------------
# Structured JSON logger
# ---------------------------------------------------------------------------
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "service": SERVICE_NAME,
            "version": SERVICE_VERSION,
            "message": record.getMessage(),
        }
        if hasattr(record, "trace_id"):
            log_entry["trace_id"] = record.trace_id
        if hasattr(record, "span_id"):
            log_entry["span_id"] = record.span_id
        if hasattr(record, "order_id"):
            log_entry["order_id"] = record.order_id
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "stacktrace": tb_module.format_exception(*record.exc_info),
            }
        return json.dumps(log_entry)

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger = logging.getLogger(SERVICE_NAME)
logger.setLevel(logging.INFO)
logger.handlers = [handler]
logger.propagate = False

logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)


def log(level, message, trace_id=None, span_id=None, order_id=None, exc_info=None, **extra):
    record = logger.makeRecord(
        SERVICE_NAME, level, "(checkout)", 0, message, (), None
    )
    if trace_id:
        record.trace_id = trace_id
    if span_id:
        record.span_id = span_id
    if order_id:
        record.order_id = order_id
    if extra:
        record.extra_fields = extra
    if exc_info:
        record.exc_info = exc_info
    logger.handle(record)


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------
class CircuitBreaker:
    """
    Simple circuit breaker with three states: CLOSED, OPEN, HALF_OPEN.

    - CLOSED: requests pass through normally. Consecutive failures are counted.
    - OPEN: requests are immediately rejected. After `recovery_timeout` seconds
      the breaker transitions to HALF_OPEN.
    - HALF_OPEN: a single probe request is allowed. If it succeeds the breaker
      closes; if it fails the breaker re-opens.
    """

    STATE_CLOSED = "closed"
    STATE_OPEN = "open"
    STATE_HALF_OPEN = "half_open"

    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = self.STATE_CLOSED
        self._failure_count = 0
        self._last_failure_time: float = 0.0
        self._half_open_in_flight = False

    @property
    def state(self) -> str:
        if self._state == self.STATE_OPEN:
            if time.time() - self._last_failure_time >= self.recovery_timeout:
                self._state = self.STATE_HALF_OPEN
                self._half_open_in_flight = False
        return self._state

    def allow_request(self) -> bool:
        s = self.state
        if s == self.STATE_CLOSED:
            return True
        if s == self.STATE_HALF_OPEN:
            if not self._half_open_in_flight:
                self._half_open_in_flight = True
                return True
            return False
        return False  # OPEN

    def record_success(self):
        self._failure_count = 0
        self._state = self.STATE_CLOSED
        self._half_open_in_flight = False

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self.failure_threshold:
            self._state = self.STATE_OPEN
        self._half_open_in_flight = False


# ---------------------------------------------------------------------------
# Custom exceptions for proper stacktraces
# ---------------------------------------------------------------------------
class InventoryServiceTimeoutError(Exception):
    """Raised when inventory-service does not respond within timeout."""
    pass


class InventoryServiceConnectionError(Exception):
    """Raised when inventory-service is unreachable (connection refused/reset)."""
    pass


class InventoryServiceError(Exception):
    """Raised when inventory-service returns a non-2xx response."""
    pass


class InventoryCircuitOpenError(Exception):
    """Raised when the inventory circuit breaker is open."""
    pass


class PaymentGatewayTimeoutError(Exception):
    """Raised when payment-service does not respond within timeout."""
    pass


class PaymentServiceConnectionError(Exception):
    """Raised when payment-service is unreachable (connection refused/reset)."""
    pass


class PaymentServiceError(Exception):
    """Raised when payment-service returns a non-2xx response."""
    pass


class PaymentCircuitOpenError(Exception):
    """Raised when the payment circuit breaker is open."""
    pass


class CheckoutBudgetExhaustedError(Exception):
    """Raised when total request budget is exhausted before payment."""
    pass


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Checkout Service", version=SERVICE_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Service URLs
INVENTORY_URL = os.getenv("INVENTORY_SERVICE_URL", "http://inventory-service:8002")
PAYMENT_URL = os.getenv("PAYMENT_SERVICE_URL", "http://payment-service:8003")
NOTIFICATION_URL = os.getenv("NOTIFICATION_SERVICE_URL", "http://notification-service:8004")
USER_URL = os.getenv("USER_SERVICE_URL", "http://user-service:8001")

# Timeout budget  (inventory default lowered from 8→5 for faster failure)
TOTAL_TIMEOUT = float(os.getenv("CHECKOUT_TOTAL_TIMEOUT", "15.0"))
INVENTORY_TIMEOUT = float(os.getenv("INVENTORY_TIMEOUT", "5.0"))
PAYMENT_TIMEOUT = float(os.getenv("PAYMENT_TIMEOUT", "8.0"))
NOTIFICATION_TIMEOUT = float(os.getenv("NOTIFICATION_TIMEOUT", "3.0"))
USER_TIMEOUT = float(os.getenv("USER_TIMEOUT", "3.0"))

NOTIFICATION_MAX_RETRIES = int(os.getenv("NOTIFICATION_MAX_RETRIES", "3"))

# Circuit breaker configuration
INVENTORY_CB_THRESHOLD = int(os.getenv("INVENTORY_CIRCUIT_BREAKER_THRESHOLD", "5"))
INVENTORY_CB_TIMEOUT = float(os.getenv("INVENTORY_CIRCUIT_BREAKER_TIMEOUT", "30"))
PAYMENT_CB_THRESHOLD = int(os.getenv("PAYMENT_CIRCUIT_BREAKER_THRESHOLD", "5"))
PAYMENT_CB_TIMEOUT = float(os.getenv("PAYMENT_CIRCUIT_BREAKER_TIMEOUT", "30"))

# Instantiate circuit breakers
inventory_cb = CircuitBreaker(
    "inventory-service",
    failure_threshold=INVENTORY_CB_THRESHOLD,
    recovery_timeout=INVENTORY_CB_TIMEOUT,
)
payment_cb = CircuitBreaker(
    "payment-service",
    failure_threshold=PAYMENT_CB_THRESHOLD,
    recovery_timeout=PAYMENT_CB_TIMEOUT,
)

# ---------------------------------------------------------------------------
# Metrics counters
# ---------------------------------------------------------------------------
_metrics = {
    "checkout_requests_total": 0,
    "checkout_success_total": 0,
    "checkout_errors_total": 0,
    "checkout_inventory_timeout_total": 0,
    "checkout_inventory_error_total": 0,
    "checkout_inventory_connection_error_total": 0,
    "checkout_inventory_circuit_open_total": 0,
    "checkout_payment_timeout_total": 0,
    "checkout_payment_error_total": 0,
    "checkout_payment_connection_error_total": 0,
    "checkout_payment_circuit_open_total": 0,
    "checkout_budget_exhausted_total": 0,
    "checkout_notification_failures_total": 0,
    "checkout_user_validation_failures_total": 0,
    "checkout_latency_seconds_sum": 0.0,
    "checkout_latency_seconds_count": 0,
    "checkout_inventory_latency_seconds_sum": 0.0,
    "checkout_inventory_latency_seconds_count": 0,
    "checkout_payment_latency_seconds_sum": 0.0,
    "checkout_payment_latency_seconds_count": 0,
}


class Item(BaseModel):
    item_id: str
    quantity: int

class CheckoutRequest(BaseModel):
    user_email: str
    items: List[Item]
    payment_method: str


# ---------------------------------------------------------------------------
# Downstream call helpers — raise typed exceptions for stacktraces
# ---------------------------------------------------------------------------
async def call_inventory(client: httpx.AsyncClient, request: CheckoutRequest, headers: dict):
    """Call inventory-service /reserve.

    Raises:
        InventoryCircuitOpenError  – circuit breaker is open
        InventoryServiceTimeoutError – timeout waiting for response
        InventoryServiceConnectionError – TCP connection failed
        InventoryServiceError – non-2xx HTTP response
    """
    if not inventory_cb.allow_request():
        raise InventoryCircuitOpenError(
            f"Circuit breaker OPEN for inventory-service "
            f"(threshold={inventory_cb.failure_threshold}, "
            f"recovery={inventory_cb.recovery_timeout}s)"
        )
    try:
        resp = await client.post(
            f"{INVENTORY_URL}/reserve",
            json=request.dict(),
            headers=headers,
            timeout=INVENTORY_TIMEOUT,
        )
        resp.raise_for_status()
        inventory_cb.record_success()
        return resp.json()
    except httpx.TimeoutException as e:
        inventory_cb.record_failure()
        raise InventoryServiceTimeoutError(
            f"inventory-service did not respond within {INVENTORY_TIMEOUT}s: {e}"
        ) from e
    except httpx.ConnectError as e:
        inventory_cb.record_failure()
        raise InventoryServiceConnectionError(
            f"inventory-service unreachable (ConnectError): {e}"
        ) from e
    except httpx.HTTPStatusError as e:
        # 5xx → record failure for circuit breaker; 4xx → don't
        if e.response.status_code >= 500:
            inventory_cb.record_failure()
        raise InventoryServiceError(
            f"inventory-service returned HTTP {e.response.status_code}: "
            f"{e.response.text}"
        ) from e
    except httpx.RequestError as e:
        # Catch-all for any other transport-level errors (DNS, reset, etc.)
        inventory_cb.record_failure()
        raise InventoryServiceConnectionError(
            f"inventory-service request failed ({type(e).__name__}): {e}"
        ) from e


async def call_payment(client: httpx.AsyncClient, payload: dict, headers: dict, timeout: float):
    """Call payment-service /process.

    Raises:
        PaymentCircuitOpenError – circuit breaker is open
        PaymentGatewayTimeoutError – timeout waiting for response
        PaymentServiceConnectionError – TCP connection failed
        PaymentServiceError – non-2xx HTTP response
    """
    if not payment_cb.allow_request():
        raise PaymentCircuitOpenError(
            f"Circuit breaker OPEN for payment-service "
            f"(threshold={payment_cb.failure_threshold}, "
            f"recovery={payment_cb.recovery_timeout}s)"
        )
    try:
        resp = await client.post(
            f"{PAYMENT_URL}/process",
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        payment_cb.record_success()
        return resp.json()
    except httpx.TimeoutException as e:
        payment_cb.record_failure()
        raise PaymentGatewayTimeoutError(
            f"payment-service did not respond within {timeout:.2f}s: {e}"
        ) from e
    except httpx.ConnectError as e:
        payment_cb.record_failure()
        raise PaymentServiceConnectionError(
            f"payment-service unreachable (ConnectError): {e}"
        ) from e
    except httpx.HTTPStatusError as e:
        if e.response.status_code >= 500:
            payment_cb.record_failure()
        raise PaymentServiceError(
            f"payment-service returned HTTP {e.response.status_code}: "
            f"{e.response.text}"
        ) from e
    except httpx.RequestError as e:
        payment_cb.record_failure()
        raise PaymentServiceConnectionError(
            f"payment-service request failed ({type(e).__name__}): {e}"
        ) from e


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "healthy", "version": SERVICE_VERSION}


@app.get("/ready")
async def ready():
    return {"ready": True}


@app.get("/metrics")
async def metrics():
    """Prometheus-compatible metrics endpoint."""
    lines = []
    for key, val in _metrics.items():
        prom_type = "counter" if "total" in key else "gauge"
        if "sum" in key or "count" in key:
            prom_type = "summary"
        lines.append(f"# HELP {key} Checkout service metric")
        lines.append(f"# TYPE {key} {prom_type}")
        lines.append(f"{key} {val}")

    # Derived metrics
    if _metrics["checkout_requests_total"] > 0:
        error_rate = _metrics["checkout_errors_total"] / _metrics["checkout_requests_total"]
        lines.append("# HELP checkout_error_rate Current error rate")
        lines.append("# TYPE checkout_error_rate gauge")
        lines.append(f"checkout_error_rate {error_rate:.4f}")

    if _metrics["checkout_latency_seconds_count"] > 0:
        avg_latency = _metrics["checkout_latency_seconds_sum"] / _metrics["checkout_latency_seconds_count