```python
import json
import logging
import os
import random
import sys
import time
import traceback as tb_module
import uuid
import asyncio
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from typing import List
import httpx

# =============================================================================
# Checkout Service v2.1.1
#
# Orchestrates checkout flow:  User → Inventory → Payment → Notification
# Enhanced with circuit breaker and retry logic for inventory service.
# =============================================================================

SERVICE_NAME = "checkout-service"
SERVICE_VERSION = "2.1.1"

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


class InventoryServiceError(Exception):
    """Raised when inventory-service returns a non-2xx response."""
    pass


class InventoryServiceConnectionError(Exception):
    """Raised when inventory-service connection fails (ConnectError)."""
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
    """Raised when circuit breaker is open for inventory service."""
    pass


# ---------------------------------------------------------------------------
# Circuit Breaker for Inventory Service
# ---------------------------------------------------------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60, expected_exception=Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN

    def call(self, func, *args, **kwargs):
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'HALF_OPEN'
                log(logging.INFO, "Circuit breaker transitioning to HALF_OPEN",
                    component="circuit-breaker", state="HALF_OPEN")
            else:
                raise CircuitBreakerOpenError("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self.on_success()
            return result
        except self.expected_exception as e:
            self.on_failure()
            raise e

    async def async_call(self, func, *args, **kwargs):
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'HALF_OPEN'
                log(logging.INFO, "Circuit breaker transitioning to HALF_OPEN",
                    component="circuit-breaker", state="HALF_OPEN")
            else:
                raise CircuitBreakerOpenError("Circuit breaker is OPEN")

        try:
            result = await func(*args, **kwargs)
            self.on_success()
            return result
        except self.expected_exception as e:
            self.on_failure()
            raise e

    def on_success(self):
        self.failure_count = 0
        if self.state == 'HALF_OPEN':
            self.state = 'CLOSED'
            log(logging.INFO, "Circuit breaker reset to CLOSED",
                component="circuit-breaker", state="CLOSED")

    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
            log(logging.WARNING, f"Circuit breaker opened after {self.failure_count} failures",
                component="circuit-breaker", state="OPEN",
                failure_count=self.failure_count)


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

# Timeout budget - Increased inventory timeout
TOTAL_TIMEOUT = float(os.getenv("CHECKOUT_TOTAL_TIMEOUT", "20.0"))
INVENTORY_TIMEOUT = float(os.getenv("INVENTORY_TIMEOUT", "12.0"))
PAYMENT_TIMEOUT = float(os.getenv("PAYMENT_TIMEOUT", "8.0"))
NOTIFICATION_TIMEOUT = float(os.getenv("NOTIFICATION_TIMEOUT", "3.0"))
USER_TIMEOUT = float(os.getenv("USER_TIMEOUT", "3.0"))

# Retry configuration
INVENTORY_MAX_RETRIES = int(os.getenv("INVENTORY_MAX_RETRIES", "3"))
INVENTORY_RETRY_BACKOFF_BASE = float(os.getenv("INVENTORY_RETRY_BACKOFF_BASE", "1.0"))
NOTIFICATION_MAX_RETRIES = int(os.getenv("NOTIFICATION_MAX_RETRIES", "3"))

# Circuit breaker for inventory service
inventory_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=60,
    expected_exception=(InventoryServiceTimeoutError, InventoryServiceError, InventoryServiceConnectionError)
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
    "checkout_inventory_retries_total": 0,
    "checkout_inventory_circuit_breaker_open_total": 0,
    "checkout_payment_timeout_total": 0,
    "checkout_payment_error_total": 0,
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
# Downstream call helpers with retry and circuit breaker
# ---------------------------------------------------------------------------
async def call_inventory_with_retry(client: httpx.AsyncClient, request: CheckoutRequest, headers: dict, trace_id: str, order_id: str):
    """Call inventory-service /reserve with exponential backoff retry and circuit breaker."""
    
    async def _single_call():
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
                f"inventory-service connection failed: {e}"
            ) from e
        except httpx.HTTPStatusError as e:
            raise InventoryServiceError(
                f"inventory-service returned HTTP {e.response.status_code}: "
                f"{e.response.text}"
            ) from e

    # Try with circuit breaker first
    try:
        return await inventory_circuit_breaker.async_call(_single_call)
    except CircuitBreakerOpenError:
        _metrics["checkout_inventory_circuit_breaker_open_total"] += 1
        log(logging.ERROR, "Inventory service circuit breaker is OPEN",
            trace_id=trace_id, order_id=order_id,
            component="checkout", step="inventory-reserve",
            downstream="inventory-service",
            error_type="CircuitBreakerOpen")
        raise InventoryServiceError("Inventory service temporarily unavailable")

    # If circuit breaker allows, try with retries
    last_exception = None
    for attempt in range(1, INVENTORY_MAX_RETRIES + 1):
        try:
            log(logging.INFO, f"Calling inventory-service (attempt {attempt}/{INVENTORY_MAX_RETRIES})",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="inventory-reserve",
                downstream="inventory-service",
                attempt=attempt,
                timeout_s=INVENTORY_TIMEOUT)
            
            return await inventory_circuit_breaker.async_call(_single_call)
            
        except (InventoryServiceTimeoutError, InventoryServiceConnectionError, InventoryServiceError) as e:
            last_exception = e
            _metrics["checkout_inventory_retries_total"] += 1
            
            if isinstance(e, InventoryServiceTimeoutError):
                _metrics["checkout_inventory_timeout_total"] += 1
                error_type = "InventoryServiceTimeout"
            elif isinstance(e, InventoryServiceConnectionError):
                _metrics["checkout_inventory_connection_error_total"] += 1
                error_type = "InventoryServiceConnectionError"
            else:
                _metrics["checkout_inventory_error_total"] += 1
                error_type = "InventoryServiceError"
            
            log(logging.WARNING, f"Inventory service call failed (attempt {attempt}/{INVENTORY_MAX_RETRIES}): {e}",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="inventory-reserve",
                downstream="inventory-service",
                error_type=error_type,
                attempt=attempt)
            
            if attempt < INVENTORY_MAX_RETRIES:
                backoff_time = INVENTORY_RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                log(logging.INFO, f"Retrying inventory call in {backoff_time:.1f}s",
                    trace_id=trace_id, order_id=order_id,
                    backoff_seconds=backoff_time)
                await asyncio.sleep(backoff_time)
        
        except CircuitBreakerOpenError:
            _metrics["checkout_inventory_circuit_breaker_open_total"] += 1
            log(logging.ERROR, "Inventory service circuit breaker opened during retry",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="inventory-reserve",
                downstream="inventory-service",
                error_type="CircuitBreakerOpen",
                attempt=attempt)
            raise InventoryServiceError("Inventory service temporarily unavailable")
    
    # All retries exhausted
    raise last_exception


async def call_payment(client: httpx.AsyncClient, payload: dict, headers: dict, timeout: float):
    """Call payment-service /process. Raises PaymentGatewayTimeoutError on timeout."""
    try:
        resp = await client.post(
            f"{PAYMENT_URL}/process",
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.TimeoutException as e:
        raise PaymentGatewayTimeoutError(
            f"payment-service did not respond within {timeout:.2f}s: {e}"
        ) from e
    except httpx.HTTPStatusError as e:
        raise PaymentServiceError(
            f"payment-service returned HTTP {e.response.status_code}: "
            f"{e.response.text}"
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
        error_rate = _metrics["checkout_errors_total"] / _