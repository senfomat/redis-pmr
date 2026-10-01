"""Exponential backoff with jitter for reconnect loops."""

import asyncio
import random

from redis_pmr.config import BackoffConfig


_JITTER = random.SystemRandom()


class ExponentialBackoff:
    """Grows the delay between attempts until :meth:`reset` is called."""

    def __init__(self, config: BackoffConfig) -> None:
        """Create a backoff with the given limits."""
        self._config = config
        self._attempt = 0

    @property
    def attempt(self) -> int:
        """Number of failed attempts since the last reset."""
        return self._attempt

    def next_delay(self) -> float:
        """Compute the delay for the next attempt and advance the counter.

        Returns:
            The delay in seconds, with jitter between 50 and 100 percent.

        """
        base = min(
            self._config.maximum,
            self._config.initial * self._config.factor**self._attempt,
        )
        self._attempt += 1
        return base * _JITTER.uniform(0.5, 1.0)

    async def sleep(self) -> None:
        """Sleep for the next delay."""
        await asyncio.sleep(self.next_delay())

    def reset(self) -> None:
        """Start over with the initial delay."""
        self._attempt = 0
