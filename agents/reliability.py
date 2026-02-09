"""
🛡️ Reliability Utilities — Resilience Patterns for Athena Platform
════════════════════════════════════════════════════════════════════
Provides enterprise-grade reliability patterns:
  - Retry with exponential backoff
  - Circuit breaker for external services
  - Rate limiting
  - Health checks
  - Graceful degradation
"""
import asyncio
import time
import logging
from functools import wraps
from typing import Optional, Callable, Any
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum

log = logging.getLogger("agents.reliability")


# ═══════════════════════════════════════════════════════════════
#  Retry Decorator with Exponential Backoff
# ═══════════════════════════════════════════════════════════════
def retry_with_backoff(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential_base: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """
    Retry decorator with exponential backoff for async functions.
    
    Args:
        max_attempts: Maximum number of attempts (including first try)
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        exponential_base: Base for exponential backoff calculation
        exceptions: Tuple of exception types to catch and retry
    
    Example:
        @retry_with_backoff(max_attempts=3, initial_delay=2)
        async def fetch_data():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts:
                        log.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise
                    
                    wait_time = min(delay, max_delay)
                    log.warning(
                        f"{func.__name__} attempt {attempt}/{max_attempts} failed: {e}. "
                        f"Retrying in {wait_time:.1f}s..."
                    )
                    await asyncio.sleep(wait_time)
                    delay *= exponential_base
            
            # Should never reach here, but just in case
            raise last_exception
        
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════════
#  Circuit Breaker Pattern
# ═══════════════════════════════════════════════════════════════
class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if recovered


@dataclass
class CircuitBreaker:
    """
    Circuit breaker pattern for external service calls.
    
    - CLOSED: Normal operation, all requests pass through
    - OPEN: Too many failures, reject requests immediately
    - HALF_OPEN: Test if service recovered with limited requests
    
    Example:
        breaker = CircuitBreaker(name="openai-api", failure_threshold=5)
        
        async def call_api():
            async with breaker:
                result = await external_api_call()
                return result
    """
    name: str
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    
    state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    failure_count: int = field(default=0, init=False)
    success_count: int = field(default=0, init=False)
    last_failure_time: Optional[datetime] = field(default=None, init=False)
    
    def __post_init__(self):
        log.info(
            f"Circuit breaker '{self.name}' initialized: "
            f"threshold={self.failure_threshold}, timeout={self.timeout_seconds}s"
        )
    
    async def __aenter__(self):
        """Context manager entry - check if circuit allows request."""
        if self.state == CircuitState.OPEN:
            # Check if timeout has elapsed
            if self.last_failure_time:
                elapsed = (datetime.now(timezone.utc) - self.last_failure_time).total_seconds()
                if elapsed >= self.timeout_seconds:
                    log.info(f"Circuit breaker '{self.name}' entering HALF_OPEN state")
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' is OPEN - "
                        f"retry in {self.timeout_seconds - elapsed:.1f}s"
                    )
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - record success or failure."""
        if exc_type is None:
            # Success
            self._record_success()
        else:
            # Failure
            self._record_failure()
        
        # Don't suppress exception
        return False
    
    def _record_success(self):
        """Record successful call."""
        self.failure_count = 0
        
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                log.info(
                    f"Circuit breaker '{self.name}' recovered - "
                    f"{self.success_count} consecutive successes"
                )
                self.state = CircuitState.CLOSED
                self.success_count = 0
    
    def _record_failure(self):
        """Record failed call."""
        self.failure_count += 1
        self.last_failure_time = datetime.now(timezone.utc)
        
        if self.state == CircuitState.HALF_OPEN:
            log.warning(f"Circuit breaker '{self.name}' failed in HALF_OPEN - reopening")
            self.state = CircuitState.OPEN
            self.failure_count = 0
        elif self.failure_count >= self.failure_threshold:
            log.error(
                f"Circuit breaker '{self.name}' OPEN after "
                f"{self.failure_count} failures"
            )
            self.state = CircuitState.OPEN
    
    def reset(self):
        """Manually reset circuit breaker to CLOSED state."""
        old_state = self.state
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        log.info(f"Circuit breaker '{self.name}' manually reset from {old_state}")


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open and rejects request."""
    pass


# ═══════════════════════════════════════════════════════════════
#  Rate Limiter
# ═══════════════════════════════════════════════════════════════
class RateLimiter:
    """
    Token bucket rate limiter for API calls.
    
    Example:
        limiter = RateLimiter(rate=10, per=60)  # 10 calls per 60 seconds
        
        async def call_api():
            await limiter.acquire()
            return await api_call()
    """
    def __init__(self, rate: int, per: float):
        """
        Args:
            rate: Number of allowed operations
            per: Time period in seconds
        """
        self.rate = rate
        self.per = per
        self.allowance = float(rate)
        self.last_check = time.time()
    
    async def acquire(self):
        """Wait until rate limit allows next operation."""
        current = time.time()
        time_passed = current - self.last_check
        self.last_check = current
        
        # Replenish tokens
        self.allowance += time_passed * (self.rate / self.per)
        if self.allowance > self.rate:
            self.allowance = self.rate
        
        if self.allowance < 1.0:
            # Need to wait
            wait_time = (1.0 - self.allowance) * (self.per / self.rate)
            log.debug(f"Rate limit: waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)
            self.allowance = 0.0
        else:
            self.allowance -= 1.0


# ═══════════════════════════════════════════════════════════════
#  Health Check
# ═══════════════════════════════════════════════════════════════
@dataclass
class HealthStatus:
    """Health status for a component or service."""
    name: str
    healthy: bool
    message: str
    last_check: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "healthy": self.healthy,
            "message": self.message,
            "last_check": self.last_check.isoformat(),
            "metadata": self.metadata,
        }


class HealthChecker:
    """
    Centralized health check coordinator.
    
    Example:
        checker = HealthChecker()
        checker.register("database", check_database_health)
        checker.register("openai", check_openai_health)
        
        status = await checker.check_all()
    """
    def __init__(self):
        self.checks: dict[str, Callable] = {}
    
    def register(self, name: str, check_func: Callable):
        """Register a health check function."""
        self.checks[name] = check_func
        log.info(f"Health check registered: {name}")
    
    async def check_all(self) -> dict[str, HealthStatus]:
        """Run all registered health checks in parallel."""
        results = {}
        tasks = {
            name: asyncio.create_task(self._safe_check(name, func))
            for name, func in self.checks.items()
        }
        
        for name, task in tasks.items():
            try:
                results[name] = await task
            except Exception as e:
                log.error(f"Health check '{name}' crashed: {e}")
                results[name] = HealthStatus(
                    name=name,
                    healthy=False,
                    message=f"Check failed: {e}",
                )
        
        return results
    
    async def _safe_check(self, name: str, func: Callable) -> HealthStatus:
        """Run a single health check with timeout."""
        try:
            return await asyncio.wait_for(func(), timeout=10.0)
        except asyncio.TimeoutError:
            return HealthStatus(
                name=name,
                healthy=False,
                message="Health check timed out after 10s",
            )
        except Exception as e:
            return HealthStatus(
                name=name,
                healthy=False,
                message=f"Check error: {e}",
            )


# ═══════════════════════════════════════════════════════════════
#  Global Circuit Breakers (Singleton Pattern)
# ═══════════════════════════════════════════════════════════════
_circuit_breakers: dict[str, CircuitBreaker] = {}

def get_circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    timeout_seconds: float = 60.0,
) -> CircuitBreaker:
    """Get or create a circuit breaker for a named service."""
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(
            name=name,
            failure_threshold=failure_threshold,
            timeout_seconds=timeout_seconds,
        )
    return _circuit_breakers[name]


def get_all_circuit_breakers() -> dict[str, CircuitBreaker]:
    """Get all active circuit breakers for monitoring."""
    return _circuit_breakers.copy()
