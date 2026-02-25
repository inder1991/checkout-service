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
#   - Separate connect vs read timeouts for downstream calls
#   - Retry with exponential backoff for inventory-service (handles ConnectError)
#   - Circuit breaker for inventory-service to fail fast during outages
#   - httpx transport with built-in retries for transient connection failures
#   - Proper handling of httpx.ConnectError as distinct from timeout/HTTP errors
#   - Async sleep in notification retries (was blocking with time.sleep)
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


class InventoryServiceUnavailableError(Exception):
    """Raised when inventory-service is unreachable (ConnectError / CrashLoopBackOff)."""
    pass


class InventoryServiceError(Exception):
    """Raised when inventory-service returns a non-2xx response."""
    pass


class PaymentGatewayTimeoutError(Exception):
    """Raised when payment-service does not respond within timeout."""
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
# Simple Circuit Breaker
# ---------------------------------------------------------------------------
class CircuitBreaker:
    """
    Lightweight circuit breaker.

    States:
      CLOSED   – requests flow normally; failures are counted.
      OPEN     – requests are rejected immediately.
      HALF_OPEN – one probe request is allowed through.

    Transitions:
      CLOSED  → OPEN      when failure_count >= failure_threshold
      OPEN    → HALF_OPEN when recovery_timeout seconds have elapsed
      HALF_OPEN → CLOSED  on success
      HALF_OPEN → OPEN    on failure
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
        self._success_count_since_half_open = 0

    @property
    def state(self) -> str:
        if self._state == self.STATE_OPEN:
            if time.time() - self._last_failure_time >= self.recovery_timeout:
                self._state = self.STATE_HALF_OPEN
                self._success_count_since_half_open = 0
                log(logging.INFO,
                    f"Circuit breaker '{self.name}' transitioning OPEN → HALF_OPEN",
                    component="circuit-breaker", circuit=self.name)
        return self._state

    def allow_request(self) -> bool:
        return self.state != self.STATE_OPEN

    def record_success(self):
        if self._state == self.STATE_HALF_OPEN:
            self._success_count_since_half_open += 1
            self._state = self.STATE_CLOSED
            self._failure_count = 0
            log(logging.INFO,
                f"Circuit breaker '{self.name}' transitioning HALF_OPEN → CLOSED",
                component="circuit-breaker", circuit=self.name)
        else:
            # In CLOSED state, reset failure count on success
            self._failure_count = 0

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._state == self.STATE_HALF_OPEN:
            self._state = self.STATE_OPEN
            log(logging.WARNING,
                f"Circuit breaker '{self.name}' transitioning HALF_OPEN → OPEN",
                component="circuit-breaker", circuit=self.name,
                failure_count=self._failure_count)
        elif self._failure_count >= self.failure_threshold:
            self._state = self.STATE_OPEN
            log(logging.WARNING,
                f"Circuit breaker '{self.name}' transitioning CLOSED → OPEN "
                f"(failures={self._failure_count}, threshold={self.failure_threshold})",
                component="circuit-breaker", circuit=self.name,
                failure_count=self._failure_count,
                failure_threshold=self.failure_threshold)


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
INVENTORY_CONNECT_TIMEOUT = float(os.getenv("INVENTORY_CONNECT_TIMEOUT", "2.0"))
INVENTORY_MAX_RETRIES = int(os.getenv("INVENTORY_MAX_RETRIES", "3"))
INVENTORY_RETRY_BACKOFF_BASE = float(os.getenv("INVENTORY_RETRY_BACKOFF_BASE", "0.5"))
PAYMENT_TIMEOUT = float(os.getenv("PAYMENT_TIMEOUT", "8.0"))
PAYMENT_CONNECT_TIMEOUT = float(os.getenv("PAYMENT_CONNECT_TIMEOUT", "2.0"))
NOTIFICATION_TIMEOUT = float(os.getenv("NOTIFICATION_TIMEOUT", "3.0"))
NOTIFICATION_CONNECT_TIMEOUT = float(os.getenv("NOTIFICATION_CONNECT_TIMEOUT", "2.0"))
USER_TIMEOUT = float(os.getenv("USER_TIMEOUT", "3.0"))
USER_CONNECT_TIMEOUT = float(os.getenv("USER_CONNECT_TIMEOUT", "2.0"))

NOTIFICATION_MAX_RETRIES = int(os.getenv("NOTIFICATION_MAX_RETRIES", "3"))

# Circuit breaker configuration
CB_FAILURE_THRESHOLD = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
CB_RECOVERY_TIMEOUT = float(os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "30.0"))

# Transport-level retries (handles transient TCP-level failures)
TRANSPORT_RETRIES = int(os.getenv("TRANSPORT_RETRIES", "2"))

# Instantiate circuit breaker for inventory-service
_inventory_circuit_breaker = CircuitBreaker(
    name="inventory-service",
    failure_threshold=CB_FAILURE_THRESHOLD,
    recovery_timeout=CB_RECOVERY_TIMEOUT,
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
    "checkout_inventory_connect_error_total": 0,
    "checkout_inventory_circuit_breaker_rejected_total": 0,
    "checkout_inventory_retry_total": 0,
    "checkout_payment_timeout_total": 0,
    "checkout_payment_error_total": 0,
    "checkout_payment_connect_error_total": 0,
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
# Helper: build httpx timeout with separate connect / read
# ---------------------------------------------------------------------------
def _make_timeout(connect: float, read: float) -> httpx.Timeout:
    return httpx.Timeout(timeout=read, connect=connect)


# ---------------------------------------------------------------------------
# Downstream call helpers — raise typed exceptions for stacktraces
# ---------------------------------------------------------------------------
async def call_inventory(
    client: httpx.AsyncClient,
    request: CheckoutRequest,
    headers: dict,
    trace_id: str = None,
    order_id: str = None,
):
    """
    Call inventory-service /reserve with retry + circuit breaker.

    Retries on ConnectError with exponential backoff.
    Raises:
      - CircuitBreakerOpenError if circuit is open
      - InventoryServiceUnavailableError after all retries exhausted on ConnectError
      - InventoryServiceTimeoutError on read timeout
      - InventoryServiceError on HTTP error responses
    """
    # Circuit breaker check
    if not _inventory_circuit_breaker.allow_request():
        raise CircuitBreakerOpenError(
            f"Circuit breaker for inventory-service is OPEN "
            f"(state={_inventory_circuit_breaker.state}). "
            f"Rejecting request to avoid cascading failure."
        )

    last_exception = None
    timeout = _make_timeout(connect=INVENTORY_CONNECT_TIMEOUT, read=INVENTORY_TIMEOUT)

    for attempt in range(1, INVENTORY_MAX_RETRIES + 1):
        try:
            if attempt > 1:
                _metrics["checkout_inventory_retry_total"] += 1
                log(logging.INFO,
                    f"Retrying inventory-service call (attempt {attempt}/{INVENTORY_MAX_RETRIES})",
                    trace_id=trace_id, order_id=order_id,
                    component="checkout", step="inventory-reserve",
                    downstream="inventory-service",
                    attempt=attempt)

            resp = await client.post(
                f"{INVENTORY_URL}/reserve",
                json=request.dict(),
                headers=headers,
                timeout=timeout,
            )
            resp.raise_for_status()
            _inventory_circuit_breaker.record_success()
            return resp.json()

        except httpx.ConnectError as e:
            last_exception = e
            _inventory_circuit_breaker.record_failure()
            _metrics["checkout_inventory_connect_error_total"] += 1
            log(logging.WARNING,
                f"inventory-service connection failed (attempt {attempt}/{INVENTORY_MAX_RETRIES}): "
                f"{type(e).__name__}: {e}",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="inventory-reserve",
                downstream="inventory-service",
                error_type="ConnectError",
                attempt=attempt,
                max_retries=INVENTORY_MAX_RETRIES)

            if attempt < INVENTORY_MAX_RETRIES:
                # Check if circuit breaker tripped during retries
                if not _inventory_circuit_breaker.allow_request():
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker for inventory-service opened during retries "
                        f"(attempt {attempt}). Aborting."
                    ) from e
                backoff = INVENTORY_RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                log(logging.INFO,
                    f"Backing off {backoff:.1f}s before retry",
                    trace_id=trace_id, order_id=order_id,
                    component="checkout", step="inventory-reserve",
                    backoff_s=backoff, attempt=attempt)
                await asyncio.sleep(backoff)
            else:
                raise InventoryServiceUnavailableError(
                    f"inventory-service unreachable after {INVENTORY_MAX_RETRIES} attempts: {e}"
                ) from e

        except httpx.TimeoutException as e:
            _inventory_circuit_breaker.record_failure()
            raise InventoryServiceTimeoutError(
                f"inventory-service did not respond within timeout "
                f"(connect={