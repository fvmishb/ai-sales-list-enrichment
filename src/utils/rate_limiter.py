"""Rate limiting utilities for API calls."""

import asyncio
import time
from collections import deque
from typing import Dict, Deque
from src.config import settings

class RateLimiter:
    """Simple token bucket rate limiter."""
    
    def __init__(self, rate_per_second: float, max_burst: int):
        self.rate_per_second = rate_per_second
        self.max_burst = max_burst
        self.tokens = max_burst
        self.last_refill_time = time.monotonic()
        self.lock = asyncio.Lock()

    async def __aenter__(self):
        async with self.lock:
            self._refill_tokens()
            while self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate_per_second
                await asyncio.sleep(sleep_time)
                self._refill_tokens()
            self.tokens -= 1

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def acquire(self):
        """Acquire a token from the rate limiter."""
        async with self.lock:
            self._refill_tokens()
            while self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate_per_second
                await asyncio.sleep(sleep_time)
                self._refill_tokens()
            self.tokens -= 1

    def _refill_tokens(self):
        now = time.monotonic()
        time_passed = now - self.last_refill_time
        new_tokens = time_passed * self.rate_per_second
        self.tokens = min(self.max_burst, self.tokens + new_tokens)
        self.last_refill_time = now


class GlobalRateLimiter:
    """Global rate limiter with domain-specific controls."""
    
    _instance = None
    _limiters: Dict[str, RateLimiter] = {}
    _domain_locks: Dict[str, asyncio.Lock] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.global_limiter = RateLimiter(settings.global_rps, settings.global_rps)
        return cls._instance

    async def get_domain_limiter(self, domain: str) -> RateLimiter:
        if domain not in self._limiters:
            async with self._get_domain_lock(domain):
                if domain not in self._limiters:  # Double-check after acquiring lock
                    self._limiters[domain] = RateLimiter(settings.domain_rps, settings.domain_rps)
        return self._limiters[domain]

    def _get_domain_lock(self, domain: str) -> asyncio.Lock:
        if domain not in self._domain_locks:
            self._domain_locks[domain] = asyncio.Lock()
        return self._domain_locks[domain]

    async def acquire_both(self, domain: str):
        """Acquire both global and domain rate limits."""
        await self.acquire_global()
        await self.acquire_domain(domain)

    async def acquire_global(self):
        """Acquire global rate limit."""
        await self.global_limiter.acquire()

    async def acquire_domain(self, domain: str):
        """Acquire domain-specific rate limit."""
        domain_limiter = await self.get_domain_limiter(domain)
        await domain_limiter.acquire()


# Global rate limiter instance
global_rate_limiter = GlobalRateLimiter()