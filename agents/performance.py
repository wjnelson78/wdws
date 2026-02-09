"""
📊 Performance Metrics Collector — Real-time Agent Performance Tracking
════════════════════════════════════════════════════════════════════════
Collects and aggregates performance metrics for agents:
  - Response time percentiles (p50, p95, p99)
  - Success/failure rates
  - Throughput (ops/second)
  - Resource utilization
  - Circuit breaker states
  - Agent coordination metrics
"""
import asyncio
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import statistics


@dataclass
class MetricsSummary:
    """Summary statistics for a metric over a time window."""
    count: int = 0
    sum: float = 0.0
    min: float = float('inf')
    max: float = float('-inf')
    p50: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    values: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    def add(self, value: float):
        """Add a new value to the metric."""
        self.count += 1
        self.sum += value
        self.min = min(self.min, value)
        self.max = max(self.max, value)
        self.values.append(value)
        
        # Update percentiles if we have enough data
        if len(self.values) >= 10:
            sorted_vals = sorted(self.values)
            self.p50 = statistics.median(sorted_vals)
            self.p95 = sorted_vals[int(len(sorted_vals) * 0.95)]
            self.p99 = sorted_vals[int(len(sorted_vals) * 0.99)]
    
    @property
    def avg(self) -> float:
        return self.sum / self.count if self.count > 0 else 0.0
    
    def to_dict(self) -> dict:
        return {
            "count": self.count,
            "avg": round(self.avg, 2),
            "min": round(self.min, 2) if self.min != float('inf') else 0,
            "max": round(self.max, 2) if self.max != float('-inf') else 0,
            "p50": round(self.p50, 2),
            "p95": round(self.p95, 2),
            "p99": round(self.p99, 2),
        }


class PerformanceCollector:
    """
    Centralized performance metrics collector for all agents.
    
    Usage:
        collector = PerformanceCollector()
        
        # Record agent run
        with collector.measure("agent-name"):
            await agent.run()
        
        # Get metrics
        metrics = collector.get_summary()
    """
    def __init__(self, window_seconds: int = 300):  # 5 minute window
        self.window_seconds = window_seconds
        self.agent_durations: Dict[str, MetricsSummary] = defaultdict(MetricsSummary)
        self.agent_successes: Dict[str, int] = defaultdict(int)
        self.agent_failures: Dict[str, int] = defaultdict(int)
        self.agent_last_run: Dict[str, datetime] = {}
        
        # Overall system metrics
        self.llm_calls: MetricsSummary = MetricsSummary()
        self.db_queries: MetricsSummary = MetricsSummary()
        self.http_requests: MetricsSummary = MetricsSummary()
    
    def measure(self, agent_id: str):
        """Context manager for measuring agent execution time."""
        return _AgentTimer(self, agent_id)
    
    def record_success(self, agent_id: str):
        """Record successful agent run."""
        self.agent_successes[agent_id] += 1
        self.agent_last_run[agent_id] = datetime.now(timezone.utc)
    
    def record_failure(self, agent_id: str):
        """Record failed agent run."""
        self.agent_failures[agent_id] += 1
        self.agent_last_run[agent_id] = datetime.now(timezone.utc)
    
    def record_llm_call(self, duration_ms: float):
        """Record LLM API call metrics."""
        self.llm_calls.add(duration_ms)
    
    def record_db_query(self, duration_ms: float):
        """Record database query metrics."""
        self.db_queries.add(duration_ms)
    
    def record_http_request(self, duration_ms: float):
        """Record HTTP request metrics."""
        self.http_requests.add(duration_ms)
    
    def get_summary(self) -> dict:
        """Get comprehensive metrics summary."""
        from reliability import get_all_circuit_breakers
        
        agent_metrics = {}
        for agent_id, durations in self.agent_durations.items():
            total = self.agent_successes[agent_id] + self.agent_failures[agent_id]
            success_rate = (self.agent_successes[agent_id] / total * 100) if total > 0 else 0
            
            agent_metrics[agent_id] = {
                "duration_ms": durations.to_dict(),
                "runs": total,
                "success": self.agent_successes[agent_id],
                "failures": self.agent_failures[agent_id],
                "success_rate": round(success_rate, 1),
                "last_run": self.agent_last_run.get(agent_id, datetime.min).isoformat(),
            }
        
        # Get circuit breaker states
        breakers = {}
        for name, breaker in get_all_circuit_breakers().items():
            breakers[name] = {
                "state": breaker.state.value,
                "failure_count": breaker.failure_count,
                "success_count": breaker.success_count,
                "last_failure": breaker.last_failure_time.isoformat() if breaker.last_failure_time else None,
            }
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "window_seconds": self.window_seconds,
            "agents": agent_metrics,
            "system": {
                "llm_calls": self.llm_calls.to_dict(),
                "db_queries": self.db_queries.to_dict(),
                "http_requests": self.http_requests.to_dict(),
            },
            "circuit_breakers": breakers,
        }
    
    def reset(self):
        """Reset all metrics."""
        self.agent_durations.clear()
        self.agent_successes.clear()
        self.agent_failures.clear()
        self.agent_last_run.clear()
        self.llm_calls = MetricsSummary()
        self.db_queries = MetricsSummary()
        self.http_requests = MetricsSummary()


class _AgentTimer:
    """Internal context manager for timing agent executions."""
    def __init__(self, collector: PerformanceCollector, agent_id: str):
        self.collector = collector
        self.agent_id = agent_id
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        self.collector.agent_durations[self.agent_id].add(duration_ms)
        
        if exc_type is None:
            self.collector.record_success(self.agent_id)
        else:
            self.collector.record_failure(self.agent_id)
        
        return False  # Don't suppress exceptions


# Global singleton
_performance_collector: Optional[PerformanceCollector] = None

def get_performance_collector() -> PerformanceCollector:
    """Get the global performance collector singleton."""
    global _performance_collector
    if _performance_collector is None:
        _performance_collector = PerformanceCollector()
    return _performance_collector
