"""
CHECKOUT SERVICE - HYBRID ARCHITECTURE
=======================================

Main checkout service with ALL 5 ORIGINAL BUGS plus microservices orchestration.

Original 5 Bugs:
1. Database connection leak
2. Memory leak (unbounded cache)
3. Null pointer / missing validation
4. API timeout (no timeout set)
5. Race condition (stock decrement) - FIXED

Plus calls to downstream microservices:
- User Service (8004)
- Inventory Service (8001)
- Payment Service (8002)
- Notification Service (8003)

Port: 8000
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, List
import asyncio
import time
import random
import json
import sys
import traceback
import io
from datetime import datetime
import os
import socket
import requests
import threading
from contextlib import contextmanager

app = FastAPI(title="Checkout Service")

# Simple JSON logging
def log_json(level: str, message: str, **context):
    log_entry = {
        '@timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'message': message,
        'log': {'level': level},
        'service': {
            'name': os.getenv('SERVICE_NAME', 'checkout-service'),
            'version': '1.0.0-buggy',
            'type': 'microservice'
        },
        'kubernetes': {
            'namespace': os.getenv('POD_NAMESPACE', 'production'),
            'pod': {'name': os.getenv('POD_NAME', socket.gethostname())}
        }
    }
    log_entry.update(context)
    print(json.dumps(log_entry), file=sys.stdout, flush=True)


def log_exception(level: str, message: str, exc: Exception = None, **context):
    """Log with full stack trace - PURE TELEMETRY ONLY"""
    
    # Production-quality structured log
    log_entry = {
        '@timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'log': {
            'level': level
        },
        'service': {
            'name': os.getenv('SERVICE_NAME', 'checkout-service'),
            'version': '1.0.0',
            'environment': os.getenv('ENVIRONMENT', 'production')
        },
        'kubernetes': {
            'namespace': os.getenv('POD_NAMESPACE', 'production'),
            'pod': {
                'name': os.getenv('POD_NAME', socket.gethostname()),
                'uid': os.getenv('POD_UID', 'unknown')
            },
            'node': {
                'name': os.getenv('NODE_NAME', 'unknown')
            }
        },
        'message': message
    }
    
    # Add trace context
    if 'trace_id' in context:
        log_entry['trace'] = {
            'id': context['trace_id']
        }
        if 'span_id' in context:
            log_entry['trace']['span_id'] = context['span_id']
    
    # Add exception details with full stack trace
    if exc:
        # Try to get the full traceback
        # Method 1: Use the exception's __traceback__ attribute
        tb_str = None
        if hasattr(exc, '__traceback__') and exc.__traceback__ is not None:
            tb_str = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        
        # Method 2: If no traceback on exception, try sys.exc_info() (current exception)
        if not tb_str:
            exc_info = sys.exc_info()
            if exc_info[0] is not None:
                tb_str = ''.join(traceback.format_exception(*exc_info))
        
        # Method 3: Fallback - at least show the exception type and message
        if not tb_str:
            tb_str = f"{type(exc).__name__}: {str(exc)}\n(No traceback available - exception may have been re-raised)"
        
        log_entry['exception'] = {
            'type': type(exc).__name__,
            'message': str(exc),
            'stacktrace': tb_str
        }
        
        # Keep for backward compatibility
        log_entry['error'] = {
            'type': type(exc).__name__,
            'message': str(exc),
            'stack_trace': tb_str
        }
        
        # Print formatted error to stderr for immediate visibility
        if level == 'ERROR':
            print(f"\n{'='*80}", file=sys.stderr, flush=True)
            print(f"ERROR: {message}", file=sys.stderr, flush=True)
            print(f"Service: {log_entry['service']['name']} | Environment: {log_entry['service']['environment']}", file=sys.stderr, flush=True)
            if 'trace_id' in context:
                print(f"Trace: {context['trace_id']}", file=sys.stderr, flush=True)
            print(f"{'='*80}", file=sys.stderr, flush=True)
            print(tb_str, file=sys.stderr, flush=True)
            print(f"{'='*80}\n", file=sys.stderr, flush=True)
    
    # Add all custom context (metrics, configuration, etc.)
    for key, value in context.items():
        if key not in ['trace_id', 'span_id']:
            log_entry[key] = value
    
    # Output structured JSON log
    print(json.dumps(log_entry), file=sys.stdout, flush=True)


# =============================================================================
# DOWNSTREAM SERVICE URLS
# =============================================================================
USER_SERVICE = os.getenv('USER_SERVICE_URL', 'http://user-service:8004')
INVENTORY_SERVICE = os.getenv('INVENTORY_SERVICE_URL', 'http://inventory-service:8001')
PAYMENT_SERVICE = os.getenv('PAYMENT_SERVICE_URL', 'http://payment-service:8002')
NOTIFICATION_SERVICE = os.getenv('NOTIFICATION_SERVICE_URL', 'http://notification-service:8003')


# =============================================================================
# BUG #1: DATABASE CONNECTION LEAK
# =============================================================================
# This list grows forever - connections are never released!

db_connections = []

def get_db_connection(trace_id: str = None):
    """
    BUG: Leaks database connections!
    Connections are added but never removed.
    """
    try:
        if len(db_connections) > 10:
            error_msg = "Connection pool exhausted: max_pool_size=10 active_connections=11 wait_timeout=30000ms"
            raise Exception(error_msg)
        
        connection = {"id": len(db_connections), "acquired_at": time.time()}
        db_connections.append(connection)
        
        log_json("INFO", "Database connection acquired", 
                trace_id=trace_id, 
                connection_id=connection['id'], 
                pool_size=len(db_connections),
                pool_max_size=10,
                connection_wait_time_ms=0)
        
        return connection
        
    except Exception as e:
        log_exception(
            "ERROR",
            "Failed to acquire database connection from pool",
            exc=e,
            trace_id=trace_id,
            span_id=f"db-conn-{int(time.time())}",
            error_code="DB_POOL_EXHAUSTED",
            pool_size=len(db_connections),
            max_pool_size=10,
            active_connections=11,
            wait_timeout_ms=30000,
            severity="critical",
            bug_id="BUG_1_CONNECTION_LEAK"
        )
        raise


# =============================================================================
# BUG #2: MEMORY LEAK
# =============================================================================
# This cache grows unbounded - no TTL, no size limit, no eviction!

order_cache = {}

def cache_order(order_id: str, order_data: dict, trace_id: str = None):
    """
    BUG: Cache grows forever!
    No TTL, no size limit, will cause OOM.
    """
    try:
        order_cache[order_id] = order_data
        
        if len(order_cache) > 50:
            heap_used_mb = len(order_cache) * 0.5
            heap_max_mb = 512
            log_json("WARN", "Cache size exceeding threshold", 
                    trace_id=trace_id, 
                    cache_size=len(order_cache),
                    threshold=50,
                    max_size=100,
                    heap_usage_mb=heap_used_mb,
                    heap_max_mb=heap_max_mb,
                    heap_usage_percent=int((heap_used_mb / heap_max_mb) * 100),
                    cache_ttl_configured=False,
                    eviction_policy="none")
        
        if len(order_cache) > 100:
            heap_used_mb = len(order_cache) * 0.5
            heap_max_mb = 512
            error_msg = f"OutOfMemoryError: heap_usage={int(heap_used_mb)}MB/{heap_max_mb}MB (95%) cache_size={len(order_cache)} cache_ttl=none gc_overhead_exceeded=true"
            raise MemoryError(error_msg)
            
    except MemoryError as e:
        log_exception(
            "ERROR",
            "Failed to allocate memory for cache entry",
            exc=e,
            trace_id=trace_id,
            span_id=f"cache-{int(time.time())}",
            error_code="OOM_CACHE_OVERFLOW",
            cache_size=len(order_cache),
            cache_max_size="unlimited",
            cache_ttl_configured=False,
            eviction_policy="none",
            heap_usage_percent=95,
            heap_used_mb=int(len(order_cache) * 0.5),
            heap_max_mb=512,
            gc_overhead_exceeded=True,
            severity="critical",
            bug_id="BUG_2_MEMORY_LEAK"
        )
        raise


# =============================================================================
# BUG #3: NULL POINTER / MISSING VALIDATION
# =============================================================================

def process_payment_internal(order: dict, trace_id: str = None):
    """
    BUG: No null checks! Will crash if user or payment_method is None.
    """
    log_json("INFO", "Processing payment validation",
            trace_id=trace_id, 
            order_id=order.get('order_id'),
            validation_step="payment_data")
    
    try:
        # BUG: No validation - missing defensive programming!
        # This should validate order["user"] is not None before accessing
        user_email = order["user"]["email"]
        payment_method = order["payment_method"]["type"]
        
        log_json("INFO", "Payment validation successful",
                trace_id=trace_id, user_email=user_email,
                payment_method=payment_method)
        
        return {"status": "success"}
        
    except (KeyError, TypeError, AttributeError) as e:
        log_exception(
            "ERROR",
            "Failed to validate payment data",
            exc=e,
            trace_id=trace_id,
            span_id=f"payment-validate-{int(time.time())}",
            error_code="NULL_VALIDATION_ERROR",
            order_id=order.get('order_id'),
            user_object_null=(order.get('user') is None),
            payment_object_null=(order.get('payment_method') is None),
            exception_type=type(e).__name__,
            severity="high",
            bug_id="BUG_3_NULL_POINTER"
        )
        raise


# =============================================================================
# BUG #4: API TIMEOUT
# =============================================================================

def call_payment_gateway(amount: float, trace_id: str = None):
    """
    BUG: No timeout on external API call! Can hang forever.
    """
    log_json("INFO", "Calling payment gateway",
            trace_id=trace_id, 
            amount=amount,
            gateway_url="https://api.payment-provider.com/v2/charge",
            gateway_provider="StripeConnect",
            timeout_configured=False)
    
    try:
        # BUG: 30% chance of slow response - no timeout configured!
        if random.random() < 0.3:
            log_json("WARN", "Payment gateway response time degraded",
                    trace_id=trace_id, 
                    response_time_ms=30000,
                    p95_latency_ms=2500,
                    timeout_configured=False)
            time.sleep(5)
            
            error_msg = "Connection timeout: read_timeout=30000ms gateway=api.payment-provider.com circuit_breaker=disabled"
            raise TimeoutError(error_msg)
        
        # BUG: 20% chance of 503 error from provider
        if random.random() < 0.2:
            error_msg = "HTTP 503 Service Unavailable: gateway=api.payment-provider.com status_page=https://status.stripe.com"
            raise ConnectionError(error_msg)
        
        log_json("INFO", "Payment gateway responded",
                trace_id=trace_id, 
                response_time_ms=850,
                http_status=200)
        
        return {"transaction_id": f"TXN-{random.randint(10000, 99999)}"}
        
    except TimeoutError as e:
        log_exception(
            "ERROR",
            "Payment gateway request timed out",
            exc=e,
            trace_id=trace_id,
            span_id=f"payment-gateway-{int(time.time())}",
            error_code="GATEWAY_TIMEOUT",
            gateway_provider="StripeConnect",
            gateway_url="https://api.payment-provider.com/v2/charge",
            timeout_configured=False,
            timeout_ms=30000,
            circuit_breaker_enabled=False,
            severity="critical",
            bug_id="BUG_4_API_TIMEOUT"
        )
        raise
    except ConnectionError as e:
        log_exception(
            "ERROR",
            "Payment gateway returned error",
            exc=e,
            trace_id=trace_id,
            span_id=f"payment-gateway-503-{int(time.time())}",
            error_code="GATEWAY_503",
            gateway_provider="StripeConnect",
            gateway_url="https://api.payment-provider.com/v2/charge",
            http_status=503,
            circuit_breaker_enabled=False,
            retry_attempted=False,
            provider_status_page="https://status.stripe.com",
            severity="critical",
            bug_id="BUG_4_API_TIMEOUT"
        )
        raise


# =============================================================================
# BUG #5: RACE CONDITION - FIXED
# =============================================================================

inventory_stock = {"item-123": 100}

# Thread-safe locking mechanism for inventory operations
stock_locks = {}  # item_id -> threading.Lock()
lock_manager_lock = threading.Lock()

def get_item_lock(item_id: str) -> threading.Lock:
    """
    Get or create a lock for a specific item.
    Thread-safe lock acquisition to prevent race conditions.
    """
    with lock_manager_lock:
        if item_id not in stock_locks:
            stock_locks[item_id] = threading.Lock()
        return stock_locks[item_id]


async def check_and_decrement_stock(item_id: str, quantity: int, trace_id: str = None):
    """
    FIXED: Atomically check and decrement stock with proper locking.
    
    This function now uses thread-safe locking to prevent race conditions
    when multiple concurrent requests attempt to decrement stock.
    
    Args:
        item_id: Product identifier
        quantity: Amount to decrement
        trace_id: Distributed tracing identifier
    
    Raises:
        RuntimeError: If insufficient stock or negative inventory detected
    """
    log_json("INFO", "Decrementing inventory stock",
            trace_id=trace_id, 
            item_id=item_id,
            requested_quantity=quantity,
            locking_mechanism="threading.Lock",
            atomic_operation=True)
    
    # Get item-specific lock to ensure atomic operations
    item_lock = get_item_lock(item_id)
    
    # Acquire lock - blocks other threads from accessing this item
    with item_lock:
        try:
            # Read current stock level
            current_stock = inventory_stock.get(item_id, 0)
            
            log_json("INFO", "Stock check under lock",
                    trace_id=trace_id,
                    item_id=item_id,
                    current_stock=current_stock,
                    requested_quantity=quantity,
                    lock_acquired=True)
            
            # CRITICAL: Validate stock availability BEFORE decrement
            # This check is now atomic with the decrement operation
            if current_stock < quantity:
                error_msg = f"Insufficient stock: item_id={item_id} available={current_stock} requested={quantity}"
                log_json("WARN", "Insufficient stock detected",
                        trace_id=trace_id,
                        item_id=item_id,
                        available_stock=current_stock,
                        requested_quantity=quantity,
                        lock_type="threading.Lock")
                raise RuntimeError(error_msg)
            
            # Calculate new stock level
            new_stock = current_stock - quantity
            
            # Additional safety check - should never happen with proper validation above
            if new_stock < 0:
                error_msg = f"Negative stock detected: item_id={item_id} stock_level={new_stock} lock_type=threading.Lock atomic=true"
                log_exception(
                    "ERROR",
                    "Negative inventory detected despite locking",
                    exc=RuntimeError(error_msg),
                    trace_id=trace_id,
                    span_id=f"race-condition-{int(time.time())}",
                    error_code="NEGATIVE_STOCK",
                    item_id=item_id,
                    stock_level=new_stock,
                    locking_mechanism="threading.Lock",
                    atomic_operation=True,
                    severity="critical",
                    bug_id="BUG_5_RACE_CONDITION_UNEXPECTED"
                )
                raise RuntimeError(error_msg)
            
            # Artificial delay to simulate database write latency
            # With proper locking, this no longer causes race conditions
            await asyncio.sleep(0.1)
            
            # Atomically update stock - protected by lock
            inventory_stock[item_id] = new_stock
            
            log_json("INFO", "Stock decremented successfully",
                    trace_id=trace_id, 
                    item_id=item_id,
                    remaining_stock=new_stock,
                    decremented_by=quantity,
                    lock_type="threading.Lock",
                    atomic_operation=True)
            
            return new_stock
                
        except RuntimeError as e:
            # Re-raise with proper logging
            log_exception(
                "ERROR",
                "Stock operation failed",
                exc=e,
                trace_id=trace_id,
                span_id=f"stock-operation-{int(time.time())}",
                error_code="STOCK_OPERATION_FAILED",
                item_id=item_id,
                requested_quantity=quantity,
                locking_mechanism="threading.Lock",
                atomic_operation=True,
                severity="high"
            )
            raise


# =============================================================================
# API MODELS
# =============================================================================

class OrderItem(BaseModel):
    item_id: str
    quantity: int


class CheckoutRequest(BaseModel):
    user_email: str
    items: List[OrderItem]
    payment_method: str


order_history = []


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/")
def root():
    return {
        "service": "checkout-service",
        "version": "1.0.0-buggy",
        "description": "Checkout service with ORIGINAL 5 bugs + microservices orchestration",
        "bugs": [
            "1. Database connection leak",
            "2. Memory leak (unbounded cache)",
            "3. Null pointer errors",
            "4. API timeout (no timeout set)",
            "5. Race condition (stock decrement) - FIXED"
        ]
    }


@app.get("/health")
def health():
    """Health check showing leaked resources"""
    
    # Check downstream services
    downstream_services = {}
    for name, url in [
        ("user-service", USER_SERVICE),
        ("inventory-service", INVENTORY_SERVICE),
        ("payment-service", PAYMENT_SERVICE),
        ("notification-service", NOTIFICATION_SERVICE)
    ]:
        try:
            response = requests.get(f"{url}/health", timeout=2)
            downstream_services[name] = "healthy" if response.status_code == 200 else "unhealthy"
        except:
            downstream_services[name] = "unreachable"
    
    status = "healthy" if len(db_connections) <= 8 and len(order_cache) <= 80 else "unhealthy"
    
    return {
        "status": status,
        "leaked_connections": len(db_connections),
        "cached_orders": len(order_cache),
        "inventory_stock": inventory_stock,
        "downstream_services": downstream_services
    }


@app.post("/checkout")
async def checkout(request: CheckoutRequest):
    """
    COMPLETE E2E CHECKOUT FLOW
    
    Contains ALL 5 ORIGINAL BUGS plus calls to downstream microservices.
    
    Flow:
    1. Validate user (downstream User Service)
    2. Check stock (downstream Inventory Service + internal bug #5 - FIXED)
    3. Acquire DB connection (internal bug #1)
    4. Cache order (internal bug #2)
    5. Validate payment data (internal bug #3)
    6. Process payment (downstream Payment Service + internal bug #4)
    7. Reserve stock (downstream Inventory Service)
    8. Send notification (downstream Notification Service)
    """
    
    order_id = f"ORD-{int(time.time())}-{random.randint(1000, 9999)}"
    trace_id = f"checkout-{order_id}"
    
    log_json("INFO", "🚀 Starting checkout flow",
            trace_id=trace_id, order_id=order_id,
            user_email=request.user_email, item_count=len(request.items),
            flow_stage="START")
    
    try:
        total_amount = 0.0
        
        # =====================================================================
        # STEP 1: Validate User (Downstream Service)
        # =====================================================================
        log_json("INFO", "➡️ Step 1: Validating user",
                trace_id=trace_id, downstream_service="user-service",
                flow_stage="USER_VALIDATION")
        
        try:
            user_response = requests.get(
                f"{USER_SERVICE}/users/{request.user_email}",
                timeout=5
            )
            # REMOVED: User validation - we only care about technical bugs
            # Always proceed even if user doesn't exist
            try:
                user_data = user_response.json()
                log_json("INFO", "✅ User check completed (validation skipped)",
                        trace_id=trace_id, user_tier=user_data.get('tier', 'unknown'))
            except:
                log_json("INFO", "✅ User check completed (validation skipped)",
                        trace_id=trace_id)
        
        except requests.exceptions.Timeout:
            log_json("WARN", "User service timeout (continuing anyway)",
                    trace_id=trace_id, 
                    downstream_service="user-service")
            # Continue - business logic errors ignored
        
        except requests.exceptions.RequestException as e:
            # REMOVED: User validation failed error - not a code bug
            log_json("WARN", "User validation failed (continuing anyway)",
                    trace_id=trace_id)
            # Continue - business logic errors ignored
        
        # =====================================================================
        # STEP 2: Check Stock (Downstream + Internal Bug #5 - FIXED)
        # =====================================================================
        log_json("INFO", "➡️ Step 2: Checking stock",
                trace_id=trace_id, downstream_service="inventory-service",
                flow_stage="STOCK_CHECK")
        
        for item in request.items:
            # Call downstream inventory service (but ignore validation - focus on bugs only)
            try:
                stock_response = requests.post(
                    f"{INVENTORY_SERVICE}/check-stock",
                    json={"item_id": item.item_id, "quantity": item.quantity},
                    timeout=5
                )
                stock_response.raise_for_status()
                stock_data = stock_response.json()
                
                # REMOVED: Stock validation - we only care about technical bugs, not business logic
                # Always proceed regardless of stock availability
                
                total_amount += 99.99 * item.quantity
                
                log_json("INFO", "✅ Stock check completed (validation skipped)",
                        trace_id=trace_id, item_id=item.item_id)
            
            except requests.exceptions.Timeout:
                log_json("WARN", "Inventory service timeout (continuing anyway)",
                        trace_id=trace_id, 
                        downstream_service="inventory-service")
                total_amount += 99.99 * item.quantity  # Continue anyway
            
            except requests.exceptions.RequestException:
                log_json("WARN", "Inventory service error (continuing anyway)",
                        trace_id=trace_id,
                        downstream_service="inventory-service")
                total_amount += 99.99 * item.quantity  # Continue anyway
            
            # FIXED Bug #5: Internal race condition check with proper locking
            try:
                await check_and_decrement_stock(item.item_id, item.quantity, trace_id)
            except RuntimeError as e:
                # Log the error with proper context
                log_exception("ERROR", "Stock operation failed",
                        exc=e,
                        trace_id=trace_id, severity="high")
                raise HTTPException(status_code=409, detail=str(e))
        
        # =====================================================================
        # STEP 3: Validate Credit (Downstream)
        # =====================================================================
        log_json("INFO", "➡️ Step 3: Validating credit limit",
                trace_id=trace_id, amount=total_amount,
                flow_stage="CREDIT_VALIDATION")
        
        try:
            credit_response = requests.post(
                f"{USER_SERVICE}/users/{request.user_email}/validate-credit",
                params={"amount": total_amount},
                timeout=5
            )
            # REMOVED: Credit validation - we only care about technical bugs, not business logic
            # Always proceed regardless of credit limit
            log_json("INFO", "✅ Credit check completed (validation skipped)", trace_id=trace_id)
        
        except requests.exceptions.HTTPError as e:
            # REMOVED: Credit limit exceeded error - not a code bug
            log_json("INFO", "Credit limit exceeded but continuing (not a code bug)",
                    trace_id=trace_id, amount=total_amount)
            pass  # Continue - business logic errors ignored
        
        # =====================================================================
        # STEP 4: Acquire Database Connection (Internal Bug #1)
        # =====================================================================
        log_json("INFO", "➡️ Step 4: Acquiring database connection",
                trace_id=trace_id, flow_stage="DB_CONNECTION")
        
        try:
            db_conn = get_db_connection(trace_id)
            log_json("INFO", "✅ Database connection acquired",
                    trace_id=trace_id, connection_id=db_conn['id'])
        except Exception as e:
            log_exception("ERROR", "Database connection failed",
                    exc=e,
                    trace_id=trace_id, severity="critical")
            raise HTTPException(status_code=503, detail="Database unavailable")
        
        # =====================================================================
        # STEP 5: Cache Order (Internal Bug #2)
        # =====================================================================
        log_json("INFO", "➡️ Step 5: Caching order",
                trace_id=trace_id, flow_stage="ORDER_CACHE")
        
        try:
            cache_order(order_id, {
                "order_id": order_id,
                "user_email": request.user_email,
                "items": [item.dict() for item in request.items],
                "total_amount": total_amount
            }, trace_id)
            log_json("INFO", "✅ Order cached", trace_id=trace_id)
        except Exception as e:
            log_exception("ERROR", "Cache order failed",
                    exc=e,
                    trace_id=trace_id, severity="critical")
            raise HTTPException(status_code=500, detail="Out of memory")
        
        # =====================================================================
        # STEP 6: Validate Payment Data (Internal Bug #3)
        # =====================================================================
        log_json("INFO", "➡️ Step 6: Validating payment data",
                trace_id=trace_id, flow_stage="PAYMENT_VALIDATION")
        
        if random.random() < 0.3:
            try:
                payment_result = process_payment_internal({
                    "order_id": order_id,
                    "user": {"email": request.user_email} if random.random() > 0.2 else None,
                    "payment_method": {"type": request.payment_method} if random.random() > 0.15 else None
                }, trace_id)
                log_json("INFO", "✅ Payment data validated", trace_id=trace_id)
            except (KeyError, TypeError, AttributeError) as e:
                # Exception already logged in process_payment_internal with full stack trace
                raise HTTPException(status_code=400, detail="Invalid payment data")
        
        # =====================================================================
        # STEP 7: Process Payment (Downstream + Internal Bug #4)
        # =====================================================================
        log_json("INFO", "➡️ Step 7: Processing payment",
                trace_id=trace_id, downstream_service="payment-service",
                amount=total_amount, flow_stage="PAYMENT_PROCESSING")
        
        # BUG #4: Call internal payment gateway (with timeout bug)
        try:
            internal_txn = call_payment_gateway(total_amount, trace_id)
        except Exception as e:
            log_exception("ERROR", "Internal payment gateway failed",
                    exc=e,
                    trace_id=trace_id, severity="critical")
            raise HTTPException(status_code=504, detail="Payment gateway timeout")
        
        # Call downstream payment service
        try:
            payment_response = requests.post(
                f"{PAYMENT_SERVICE}/process-payment",
                json={
                    "order_id": order_id,
                    "amount": total_amount,
                    "payment_method": request.payment_method,
                    "user_email": request.user_email
                },
                timeout=10
            )
            payment_response.raise_for_status()
            payment_data = payment_response.json()
            
            log_json("INFO", "✅ Payment processed",
                    trace_id=trace_id,
                    transaction_id=payment_data.get('transaction_id'))
        
        except requests.exceptions.Timeout:
            log_json("WARN", "Payment service timeout (continuing anyway)",
                    trace_id=trace_id,
                    downstream_service="payment-service")
            payment_data = {"transaction_id": f"TXN-FALLBACK-{random.randint(10000, 99999)}"}
            # Continue - downstream errors don't block internal bugs
        
        except requests.exceptions.RequestException:
            log_json("WARN", "Payment service error (continuing