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

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from typing import List
import httpx
import asyncio

# =============================================================================
# Checkout Service v2.1.0
#
# Orchestrates checkout flow:  User → Inventory → Payment → Notification
# All calls are SEQUENTIAL (synchronous within the request path).
# =============================================================================

SERVICE_NAME = "checkout-service"
SERVICE_VERSION = "2.1.0"

# ---------------------------------------------------------------------------
# Circuit Breaker Implementation
# ---------------------------------------------------------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30, expected_exception=Exception):
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
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self.on_success()
            return result
        except self.expected_exception as e:
            self.on_failure()
            raise e
    
    async def acall(self, func, *args, **kwargs):
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'HALF_OPEN'
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            self.on_success()
            return result
        except self.expected_exception as e:
            self.on_failure()
            raise e
    
    def on_success(self):
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

# Global circuit breakers for downstream services
inventory_circuit_breaker = CircuitBreaker(
    failure_threshold=3, 
    recovery_timeout=30, 
    expected_exception=(httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)
)

payment_circuit_breaker = CircuitBreaker(
    failure_threshold=3, 
    recovery_timeout=30, 
    expected_exception=(httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)
)

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


class InventoryServiceUnavailableError(Exception):
    """Raised when inventory-service is completely unavailable (circuit breaker open)."""
    pass


class PaymentGatewayTimeoutError(Exception):
    """Raised when payment-service does not respond within timeout."""
    pass


class PaymentServiceError(Exception):
    """Raised when payment-service returns a non-2xx response."""
    pass


class PaymentServiceUnavailableError(Exception):
    """Raised when payment-service is completely unavailable (circuit breaker open)."""
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

# Timeout budget
TOTAL_TIMEOUT = float(os.getenv("CHECKOUT_TOTAL_TIMEOUT", "15.0"))
INVENTORY_TIMEOUT = float(os.getenv("INVENTORY_TIMEOUT", "3.0"))
PAYMENT_TIMEOUT = float(os.getenv("PAYMENT_TIMEOUT", "8.0"))
NOTIFICATION_TIMEOUT = float(os.getenv("NOTIFICATION_TIMEOUT", "3.0"))
USER_TIMEOUT = float(os.getenv("USER_TIMEOUT", "3.0"))

NOTIFICATION_MAX_RETRIES = int(os.getenv("NOTIFICATION_MAX_RETRIES", "3"))

# ---------------------------------------------------------------------------
# Metrics counters
# ---------------------------------------------------------------------------
_metrics = {
    "checkout_requests_total": 0,
    "checkout_success_total": 0,
    "checkout_errors_total": 0,
    "checkout_inventory_timeout_total": 0,
    "checkout_inventory_error_total": 0,
    "checkout_inventory_circuit_breaker_open_total": 0,
    "checkout_payment_timeout_total": 0,
    "checkout_payment_error_total": 0,
    "checkout_payment_circuit_breaker_open_total": 0,
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
# Health check helper
# ---------------------------------------------------------------------------
async def check_downstream_health():
    """Check if critical downstream services are available."""
    health_status = {
        "inventory": False,
        "payment": False,
        "user": False,
        "notification": False
    }
    
    async with httpx.AsyncClient() as client:
        # Check inventory service
        try:
            resp = await client.get(f"{INVENTORY_URL}/health", timeout=2.0)
            health_status["inventory"] = resp.status_code == 200
        except:
            pass
            
        # Check payment service
        try:
            resp = await client.get(f"{PAYMENT_URL}/health", timeout=2.0)
            health_status["payment"] = resp.status_code == 200
        except:
            pass
            
        # Check user service
        try:
            resp = await client.get(f"{USER_URL}/health", timeout=2.0)
            health_status["user"] = resp.status_code == 200
        except:
            pass
            
        # Check notification service
        try:
            resp = await client.get(f"{NOTIFICATION_URL}/health", timeout=2.0)
            health_status["notification"] = resp.status_code == 200
        except:
            pass
    
    return health_status


# ---------------------------------------------------------------------------
# Downstream call helpers with circuit breakers and exponential backoff
# ---------------------------------------------------------------------------
async def call_inventory_with_backoff(client: httpx.AsyncClient, request: CheckoutRequest, headers: dict, max_retries=2):
    """Call inventory-service /reserve with exponential backoff and circuit breaker."""
    for attempt in range(max_retries + 1):
        try:
            return await inventory_circuit_breaker.acall(call_inventory, client, request, headers)
        except Exception as e:
            if "Circuit breaker is OPEN" in str(e):
                raise InventoryServiceUnavailableError(
                    "Inventory service is currently unavailable (circuit breaker open)"
                ) from e
            
            if attempt < max_retries:
                backoff_time = (2 ** attempt) * 0.1  # 0.1s, 0.2s, 0.4s
                log(logging.WARNING, 
                    f"Inventory call attempt {attempt + 1} failed, retrying in {backoff_time}s",
                    headers.get("x-request-id"), headers.get("x-span-id"), headers.get("x-order-id"),
                    error_type=type(e).__name__, attempt=attempt + 1, backoff_s=backoff_time)
                await asyncio.sleep(backoff_time)
            else:
                raise


async def call_inventory(client: httpx.AsyncClient, request: CheckoutRequest, headers: dict):
    """Call inventory-service /reserve. Raises specific exceptions for different failure modes."""
    try:
        resp = await client.post(
            f"{INVENTORY_URL}/reserve",
            json=request.dict(),
            headers=headers,
            timeout=INVENTORY_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.ConnectError as e:
        raise InventoryServiceError(
            f"inventory-service connection failed: {e}"
        ) from e
    except httpx.TimeoutException as e:
        raise InventoryServiceTimeoutError(
            f"inventory-service did not respond within {INVENTORY_TIMEOUT}s: {e}"
        ) from e
    except httpx.HTTPStatusError as e:
        raise InventoryServiceError(
            f"inventory-service returned HTTP {e.response.status_code}: "
            f"{e.response.text}"
        ) from e


async def call_payment_with_backoff(client: httpx.AsyncClient, payload: dict, headers: dict, timeout: float, max_retries=2):
    """Call payment-service /process with exponential backoff and circuit breaker."""
    for attempt in range(max_retries + 1):
        try:
            return await payment_circuit_breaker.acall(call_payment, client, payload, headers, timeout)
        except Exception as e:
            if "Circuit breaker is OPEN" in str(e):
                raise PaymentServiceUnavailableError(
                    "Payment service is currently unavailable (circuit breaker open)"
                ) from e
            
            if attempt < max_retries:
                backoff_time = (2 ** attempt) * 0.1  # 0.1s, 0.2s, 0.4s
                log(logging.WARNING, 
                    f"Payment call attempt {attempt + 1} failed, retrying in {backoff_time}s",
                    headers.get("x-request-id"), headers.get("x-span-id"), headers.get("x-order-id"),
                    error_type=type(e).__name__, attempt=attempt + 1, backoff_s=backoff_time)
                await asyncio.sleep(backoff_time)
            else:
                raise


async def call_payment(client: httpx.AsyncClient, payload: dict, headers: dict, timeout: float):
    """Call payment-service /process. Raises specific exceptions for different failure modes."""
    try:
        resp = await client.post(
            f"{PAYMENT_URL}/process",
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.ConnectError as e:
        raise PaymentServiceError(
            f"payment-service connection failed: {e}"
        ) from e
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
    """Readiness check that validates downstream service availability."""
    health_status = await check_downstream_health()
    
    # Service is ready if critical services (inventory, payment) are available
    critical_services_healthy = health_status["inventory"] and health_status["payment"]
    
    if critical_services_healthy:
        return {
            "ready": True, 
            "downstream_health": health_status,
            "circuit_breakers": {
                "inventory": inventory_circuit_breaker.state,
                "payment": payment_circuit_breaker.state
            }
        }
    else:
        return JSONResponse(
            status_code=503,
            content={
                "ready": False, 
                "downstream_health": health_status,
                "circuit_breakers": {
                    "inventory": inventory_circuit_breaker.state,
                    "payment": payment