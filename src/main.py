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
# Checkout Service v2.1.0
#
# Orchestrates checkout flow:  User → Inventory → Payment → Notification
# All calls are SEQUENTIAL (synchronous within the request path).
# =============================================================================

SERVICE_NAME = "checkout-service"
SERVICE_VERSION = "2.1.0"

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


class PaymentGatewayTimeoutError(Exception):
    """Raised when payment-service does not respond within timeout."""
    pass


class PaymentServiceError(Exception):
    """Raised when payment-service returns a non-2xx response."""
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
INVENTORY_TIMEOUT = float(os.getenv("INVENTORY_TIMEOUT", "8.0"))
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
# Downstream call helpers — raise typed exceptions for stacktraces
# ---------------------------------------------------------------------------
async def call_inventory(client: httpx.AsyncClient, request: CheckoutRequest, headers: dict):
    """Call inventory-service /reserve. Raises InventoryServiceTimeoutError on timeout."""
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
    except httpx.HTTPStatusError as e:
        raise InventoryServiceError(
            f"inventory-service returned HTTP {e.response.status_code}: "
            f"{e.response.text}"
        ) from e


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
        error_rate = _metrics["checkout_errors_total"] / _metrics["checkout_requests_total"]
        lines.append(f"# HELP checkout_error_rate Current error rate")
        lines.append(f"# TYPE checkout_error_rate gauge")
        lines.append(f"checkout_error_rate {error_rate:.4f}")

    if _metrics["checkout_latency_seconds_count"] > 0:
        avg_latency = _metrics["checkout_latency_seconds_sum"] / _metrics["checkout_latency_seconds_count"]
        lines.append(f"# HELP checkout_avg_latency_seconds Average checkout latency")
        lines.append(f"# TYPE checkout_avg_latency_seconds gauge")
        lines.append(f"checkout_avg_latency_seconds {avg_latency:.4f}")

    if _metrics["checkout_inventory_latency_seconds_count"] > 0:
        avg_inv = _metrics["checkout_inventory_latency_seconds_sum"] / _metrics["checkout_inventory_latency_seconds_count"]
        lines.append(f"# HELP checkout_inventory_avg_latency_seconds Average inventory call latency")
        lines.append(f"# TYPE checkout_inventory_avg_latency_seconds gauge")
        lines.append(f"checkout_inventory_avg_latency_seconds {avg_inv:.4f}")

    lines.append(f'# HELP service_info Service metadata')
    lines.append(f'# TYPE service_info gauge')
    lines.append(f'service_info{{version="{SERVICE_VERSION}"}} 1')

    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain")


@app.post("/checkout")
async def checkout(request: CheckoutRequest, req: Request):
    _metrics["checkout_requests_total"] += 1
    request_start = time.time()
    order_id = f"ORD-{random.randint(10000, 99999)}"
    trace_id = req.headers.get("x-request-id", uuid.uuid4().hex[:16])
    span_id = uuid.uuid4().hex[:8]

    log(logging.INFO, "Checkout initiated",
        trace_id=trace_id, span_id=span_id, order_id=order_id,
        user_email=request.user_email,
        items=[i.dict() for i in request.items],
        payment_method=request.payment_method,
        component="checkout", step="start")

    headers = {
        "x-request-id": trace_id,
        "x-span-id": span_id,
        "x-order-id": order_id,
    }

    async with httpx.AsyncClient() as client:

        # ==================================================================
        # STEP 1: Validate user
        # ==================================================================
        try:
            step_start = time.time()
            log(logging.INFO, "Calling user-service for validation",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="user-validation",
                downstream="user-service")

            user_resp = await client.get(
                f"{USER_URL}/health", headers=headers, timeout=USER_TIMEOUT)
            user_resp.raise_for_status()

            log(logging.INFO,
                f"User validation completed in {time.time() - step_start:.3f}s",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="user-validation",
                downstream="user-service",
                response_time_s=round(time.time() - step_start, 3))
        except Exception as e:
            _metrics["checkout_user_validation_failures_total"] += 1
            log(logging.WARNING,
                f"User service unavailable: {type(e).__name__} — proceeding",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="user-validation",
                downstream="user-service",
                error_type=type(e).__name__)

        # ==================================================================
        # STEP 2: Reserve inventory (synchronous — the bottleneck)
        # ==================================================================
        inv_start = time.time()
        try:
            log(logging.INFO, "Calling inventory-service for stock reservation",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="inventory-reserve",
                downstream="inventory-service",
                timeout_s=INVENTORY_TIMEOUT)

            inv_result = await call_inventory(client, request, headers)
            inv_elapsed = time.time() - inv_start
            _metrics["checkout_inventory_latency_seconds_sum"] += inv_elapsed
            _metrics["checkout_inventory_latency_seconds_count"] += 1

            log(logging.INFO,
                f"Inventory reservation completed in {inv_elapsed:.3f}s",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="inventory-reserve",
                downstream="inventory-service",
                response_time_s=round(inv_elapsed, 3))

        except InventoryServiceTimeoutError:
            inv_elapsed = time.time() - inv_start
            total_elapsed = time.time() - request_start
            _metrics["checkout_inventory_timeout_total"] += 1
            _metrics["checkout_errors_total"] += 1
            log(logging.ERROR,
                f"Inventory service timeout after {inv_elapsed:.2f}s "
                f"(total request time: {total_elapsed:.2f}s)",
                trace_id=trace_id, order_id=order_id,
                exc_info=sys.exc_info(),
                component="checkout", step="inventory-reserve",
                downstream="inventory-service",
                error_type="InventoryServiceTimeout",
                timeout_s=INVENTORY_TIMEOUT,
                response_time_s=round(inv_elapsed, 2),
                total_elapsed_s=round(total_elapsed, 2))
            raise HTTPException(status_code=500, detail="Internal Server Error")

        except InventoryServiceError:
            inv_elapsed = time.time() - inv_start
            _metrics["checkout_inventory_error_total"] += 1
            _metrics["checkout_errors_total"] += 1
            log(logging.ERROR,
                f"Inventory service returned error after {inv_elapsed:.2f}s",
                trace_id=trace_id, order_id=order_id,
                exc_info=sys.exc_info(),
                component="checkout", step="inventory-reserve",
                downstream="inventory-service",
                error_type="InventoryServiceError",
                response_time_s=round(inv_elapsed, 2))
            raise HTTPException(status_code=500, detail="Internal Server Error")

        # ==================================================================
        # STEP 3: Process payment
        # If inventory was slow, remaining budget is nearly gone.
        # Checkout does NOT know why — blames payment.
        # ==================================================================
        elapsed_so_far = time.time() - request_start
        remaining_budget = TOTAL_TIMEOUT - elapsed_so_far

        if remaining_budget <= 0.5:
            try:
                raise CheckoutBudgetExhaustedError(
                    f"Request budget exhausted: {elapsed_so_far:.2f}s elapsed "
                    f"of {TOTAL_TIMEOUT}s total. Cannot initiate payment call."
                )
            except CheckoutBudgetExhaustedError:
                _metrics["checkout_budget_exhausted_total"] += 1
                _metrics["checkout_payment_timeout_total"] += 1
                _metrics["checkout_errors_total"] += 1
                log(logging.ERROR,
                    f"Payment Gateway Timeout: request budget exhausted "
                    f"({elapsed_so_far:.2f}s elapsed, {TOTAL_TIMEOUT}s budget). "
                    f"Unable to initiate payment call.",
                    trace_id=trace_id, order_id=order_id,
                    exc_info=sys.exc_info(),
                    component="checkout", step="payment-process",
                    downstream="payment-service",
                    error_type="GatewayTimeout",
                    total_elapsed_s=round(elapsed_so_far, 2),
                    budget_remaining_s=round(remaining_budget, 2))
                raise HTTPException(status_code=504, detail="Gateway Timeout")

        effective_timeout = min(PAYMENT_TIMEOUT, remaining_budget)
        pay_start = time.time()

        try:
            log(logging.INFO, "Calling payment-service for payment processing",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="payment-process",
                downstream="payment-service",
                effective_timeout_s=round(effective_timeout, 2),
                budget_remaining_s=round(remaining_budget, 2))

            payment_payload = {
                "amount": round(random.uniform(10.0, 500.0), 2),
                "currency": "USD",
                "payment_method": request.payment_method,
                "user_email": request.user_email,
            }

            pay_result = await call_payment(client, payment_payload, headers, effective_timeout)
            pay_elapsed = time.time() - pay_start
            _metrics["checkout_payment_latency_seconds_sum"] += pay_elapsed
            _metrics["checkout_payment_latency_seconds_count"] += 1

            log(logging.INFO,
                f"Payment processed in {pay_elapsed:.3f}s, "
                f"txn={pay_result.get('transaction_id')}",
                trace_id=trace_id, order_id=order_id,
                component="checkout", step="payment-process",
                downstream="payment-service",
                response_time_s=round(pay_elapsed, 3),
                transaction_id=pay_result.get("transaction_id"))

        except PaymentGatewayTimeoutError:
            pay_elapsed = time.time() - pay_start
            total_elapsed = time.time() - request_start
            _metrics["checkout_payment_timeout_total"] += 1
            _metrics["checkout_errors_total"] += 1
            log(logging.ERROR,
                f"Payment Gateway Timeout: payment-service did not respond "
                f"in {pay_elapsed:.2f}s (effective timeout was {effective_timeout:.2f}s, "
                f"total request time: {total_elapsed:.2f}s)",
                trace_id=trace_id, order_id=order_id,
                exc_info=sys.exc_info(),
                component="checkout", step="payment-process",
                downstream="payment-service",
                error_type="GatewayTimeout",
                effective_timeout_s=round(effective_timeout, 2),
                response_time_s=round(pay_elapsed, 2),
                total_elapsed_s=round(total_elapsed, 2))
            raise HTTPException(status_code=504, detail="Gateway Timeout")

        except PaymentServiceError:
            pay_elapsed = time.time() - pay_start
            _metrics["checkout_payment_error_total"] += 1
            _metrics["checkout_errors_total"] += 1
            log(logging.ERROR,
                f"Payment service returned error after {pay_elapsed:.2f}s",
                trace_id=trace_id, order_id=order_id,
                exc_info=sys.exc_info(),
                component="checkout", step="payment-process",
                downstream="payment-service",
                error_type="PaymentServiceError",
                response_time_s=round(pay_elapsed, 2))
            raise HTTPException(status_code=500, detail="Internal Server Error")

        # ==================================================================
        # STEP 4: Notification (fire-and-forget with retries)
        # ==================================================================
        for attempt in range(1, NOTIFICATION_MAX_RETRIES + 1):
            try:
                log(logging.INFO,
                    f"Sending order confirmation (attempt {attempt}/{NOTIFICATION_MAX_RETRIES})",
                    trace_id=trace_id, order_id=order_id,
                    component="checkout", step="notification",
                    downstream="notification-service",
                    attempt=attempt)

                notif_resp = await client.post(
                    f"{NOTIFICATION_URL}/email",
                    json={
                        "to": request.user_email,
                        "subject": f"Order Confirmation - {order_id}",
                        "body": f"Your order {order_id} has been confirmed!",
                        "order_id": order_id,
                    },
                    headers=headers,
                    timeout=NOTIFICATION_TIMEOUT,
                )
                notif_resp.raise_for_status()
                log(logging.INFO, "Order confirmation sent",
                    trace_id=trace_id, order_id=order_id,
                    component="checkout", step="notification",
                    downstream="notification-service")
                break
            except Exception as e:
                log(logging.WARNING,
                    f"Notification attempt {attempt} failed: {type(e).__name__}: {e}",
                    trace_id=trace_id, order_id=order_id,
                    component="checkout", step="notification",
                    downstream="notification-service",
                    error_type=type(e).__name__,
                    attempt=attempt)
                if attempt < NOTIFICATION_MAX_RETRIES:
                    time.sleep(0.5 * attempt)

    # ==================================================================
    # SUCCESS
    # ==================================================================
    total_elapsed = time.time() - request_start
    _metrics["checkout_success_total"] += 1
    _metrics["checkout_latency_seconds_sum"] += total_elapsed
    _metrics["checkout_latency_seconds_count"] += 1
    log(logging.INFO,
        f"Checkout completed successfully in {total_elapsed:.3f}s",
        trace_id=trace_id, order_id=order_id,
        component="checkout", step="complete",
        user_email=request.user_email,
        total_elapsed_s=round(total_elapsed, 3),
        transaction_id=pay_result.get("transaction_id"))

    return {
        "success": True,
        "order_id": order_id,
        "message": "Order placed successfully",
        "transaction_id": pay_result.get("transaction_id"),
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log(logging.ERROR, f"Unhandled exception: {type(exc).__name__}: {exc}",
        exc_info=sys.exc_info(),
        component="checkout", error_type=type(exc).__name__)
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
