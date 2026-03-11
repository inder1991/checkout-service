```python
import json
import logging
import os
import random
import sys
import time
import traceback as tb_module
import uuid
from datetime import datetime, timezone
from threading import Lock

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
# v2.2.0 — Added circuit breaker, retry with backoff, and ConnectError handling
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
# Custom exceptions for proper stacktraces
# ---------------------------------------------------------------------------
class InventoryServiceTimeoutError(Exception):
    """Raised when inventory-service does not respond within timeout."""
    pass


class InventoryServiceConnectionError(Exception):
    """Raised when inventory-service is unreachable (ConnectError)."""
    pass


class InventoryServiceError(Exception):
    """Raised when inventory-service returns a non-2xx response."""
    pass


class PaymentGatewayTimeoutError(Exception):
    """Raised when payment-service does not respond within timeout."""
    pass


class PaymentServiceConnectionError(Exception):
    """Raised when payment-service is unreachable (ConnectError)."""
    pass


class PaymentServiceError(Exception):
    """Raised when payment-service returns a non-2xx response."""
    pass


class CheckoutBudgetExhaustedError(Exception):
    """Raised when total request budget is exhausted before payment."""
    pass


class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is open and calls are rejected."""
    pass


# ---------------------------------------------------------------------------
# Circuit Breaker implementation
# ---------------------------------------------------------------------------
class CircuitBreaker:
    """
    Simple circuit breaker with three states: CLOSED, OPEN, HALF_OPEN.

    - CLOSED: requests pass through normally. Failures are counted.
    - OPEN: requests are immediately rejected. After `recovery_timeout` seconds,
      transitions to HALF_OPEN.
    - HALF_OPEN: a single probe request is allowed. If it succeeds, go CLOSED.
      If it fails, go back to OPEN.
    """

    STATE_CLOSED = "closed"
    STATE_OPEN = "open"
    STATE_HALF_OPEN = "half_open"

    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0, half_open_max_calls: int = 1):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state = self.STATE_CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._half_open_calls = 0
        self._lock = Lock()

    @property
    def state(self) -> str:
        with self._lock:
            if self._state == self.STATE_OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = self.STATE_HALF_OPEN
                    self._half_open_calls = 0
                    log(logging.INFO,
                        f"Circuit breaker '{self.name}' transitioning OPEN -> HALF_OPEN",
                        component="circuit-breaker", circuit=self.name)
            return self._state

    def allow_request(self) -> bool:
        state = self.state
        if state == self.STATE_CLOSED:
            return True
        if state == self.STATE_HALF_OPEN:
            with self._lock:
                if self._half_open_calls < self.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                return False
        return False  # OPEN

    def record_success(self):
        with self._lock:
            if self._state == self.STATE_HALF_OPEN:
                self._state = self.STATE_CLOSED
                self._failure_count = 0
                self._success_count = 0
                log(logging.INFO,
                    f"Circuit breaker '{self.name}' transitioning HALF_OPEN -> CLOSED",
                    component="circuit-breaker", circuit=self.name)
            else:
                self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == self.STATE_HALF_OPEN:
                self._state = self.STATE_OPEN
                log(logging.WARNING,
                    f"Circuit breaker '{self.name}' transitioning HALF_OPEN -> OPEN",
                    component="circuit-breaker", circuit=self.name,
                    failure_count=self._failure_count)
            elif self._failure_count >= self.failure_threshold:
                if self._state != self.STATE_OPEN:
                    self._state = self.STATE_OPEN
                    log(logging.ERROR,
                        f"Circuit breaker '{self.name}' OPENED after {self._failure_count} failures",
                        component="circuit-breaker", circuit=self.name,
                        failure_count=self._failure_count,
                        recovery_timeout_s=self.recovery_timeout)


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

# Timeout budget
TOTAL_TIMEOUT = float(os.getenv("CHECKOUT_TOTAL_TIMEOUT", "15.0"))
INVENTORY_TIMEOUT = float(os.getenv("INVENTORY_TIMEOUT", "8.0"))
PAYMENT_TIMEOUT = float(os.getenv("PAYMENT_TIMEOUT", "8.0"))
NOTIFICATION_TIMEOUT = float(os.getenv("NOTIFICATION_TIMEOUT", "3.0"))
USER_TIMEOUT = float(os.getenv("USER_TIMEOUT", "3.0"))

NOTIFICATION_MAX_RETRIES = int(os.getenv("NOTIFICATION_MAX_RETRIES", "3"))

# Retry configuration
INVENTORY_RETRY_COUNT = int(os.getenv("INVENTORY_RETRY_COUNT", "3"))
INVENTORY_RETRY_BASE_DELAY = float(os.getenv("INVENTORY_RETRY_BASE_DELAY", "0.5"))
INVENTORY_RETRY_MAX_DELAY = float(os.getenv("INVENTORY_RETRY_MAX_DELAY", "4.0"))

PAYMENT_RETRY_COUNT = int(os.getenv("PAYMENT_RETRY_COUNT", "2"))
PAYMENT_RETRY_BASE_DELAY = float(os.getenv("PAYMENT_RETRY_BASE_DELAY", "0.5"))

# Circuit breaker configuration
INVENTORY_CB_FAILURE_THRESHOLD = int(os.getenv("INVENTORY_CIRCUIT_BREAKER_THRESHOLD", "5"))
INVENTORY_CB_RECOVERY_TIMEOUT = float(os.getenv("INVENTORY_CIRCUIT_BREAKER_TIMEOUT", "30"))

PAYMENT_CB_FAILURE_THRESHOLD = int(os.getenv("PAYMENT_CIRCUIT_BREAKER_THRESHOLD", "5"))
PAYMENT_CB_RECOVERY_TIMEOUT = float(os.getenv("PAYMENT_CIRCUIT_BREAKER_TIMEOUT", "30"))

# Initialize circuit breakers
inventory_circuit_breaker = CircuitBreaker(
    name="inventory-service",
    failure_threshold=INVENTORY_CB_FAILURE_THRESHOLD,
    recovery_timeout=INVENTORY_CB_RECOVERY_TIMEOUT,
)

payment_circuit_breaker = CircuitBreaker(
    name="payment-service",
    failure_threshold=PAYMENT_CB_FAILURE_THRESHOLD,
    recovery_timeout=PAYMENT_CB_RECOVERY_TIMEOUT,
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
    "checkout_inventory_circuit_breaker_rejected_total": 0,
    "checkout_inventory_retry_total": 0,
    "checkout_payment_timeout_total": 0,
    "checkout_payment_error_total": 0,
    "checkout_payment_connection_error_total": 0,
    "checkout_payment_circuit_breaker_rejected_total": 0,
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
# Retry helper with exponential backoff + jitter
# ---------------------------------------------------------------------------
import asyncio

async def retry_with_backoff(coro_factory, max_retries: int, base_delay: float,
                              max_delay: float, retryable_exceptions: tuple,
                              trace_id: str = None, order_id: str = None,
                              step: str = "", downstream: str = ""):
    """
    Retry an async call with exponential backoff and jitter.
    `coro_factory` is a callable that returns a new coroutine each time.
    """
    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            return await coro_factory()
        except retryable_exceptions as e:
            last_exception = e
            if attempt < max_retries:
                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                jitter = random.uniform(0, delay * 0.5)
                wait_time = delay + jitter
                if "inventory" in step:
                    _metrics["checkout_inventory_retry_total"] = _metrics.get("checkout_inventory_retry_total", 0) + 1
                log(logging.WARNING,
                    f"Retryable error on {downstream} (attempt {attempt}/{max_retries}): "
                    f"{type(e).__name__}: {e}. Retrying in {wait_time:.2f}s",
                    trace_id=trace_id, order_id=order_id,
                    component="checkout", step=step,
                    downstream=downstream,
                    error_type=type(e).__name__,
                    attempt=attempt,
                    retry_delay_s=round(wait_time, 2))
                await asyncio.sleep(wait_time)
            else:
                log(logging.ERROR,
                    f"All {max_retries} retry attempts exhausted for {downstream}: "
                    f"{type(e).__name__}: {e}",
                    trace_id=trace_id, order_id=order_id,
                    component="checkout", step=step,
                    downstream=downstream,
                    error_type=type(e).__name__,
                    attempt=attempt)
    raise last_exception


# ---------------------------------------------------------------------------
# Downstream call helpers — raise typed exceptions for stacktraces
# ---------------------------------------------------------------------------
async def call_inventory(client: httpx.AsyncClient, request: CheckoutRequest, headers: dict):
    """Call inventory-service /reserve. Raises typed exceptions on failure."""
    try:
        resp = await client.post(
            f"{INVENTORY_URL}/reserve",
            json=request.dict(),
            headers=headers,
            timeout=INVENTORY_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.TimeoutException as e:
        raise InventoryServiceTimeoutError(
            f"inventory-service did not respond within {INVENTORY_TIMEOUT}s: {e}"
        ) from e
    except httpx.ConnectError as e:
        raise InventoryServiceConnectionError(
            f"inventory-service is unreachable (connection refused/failed): {e}"
        ) from e
    except httpx.HTTPStatusError as e:
        raise InventoryServiceError(
            f"inventory-service returned HTTP {e.response.status_code}: "
            f"{e.response.text}"
        ) from e
    except httpx.TransportError as e:
        raise InventoryServiceConnectionError(
            f"inventory-service transport error: {type(e).__name__}: {e}"