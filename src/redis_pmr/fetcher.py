"""Reads the current state of dirty keys from the local server."""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError

from redis_pmr.backoff import ExponentialBackoff
from redis_pmr.buffer import CoalescingBuffer


logger = logging.getLogger(__name__)

# None means "all targets".
type TargetSet = frozenset[str] | None

NO_EXPIRY = -1
KEY_MISSING = -2


def merge_targets(old: TargetSet, new: TargetSet) -> TargetSet:
    """Combine two target sets, where None (all targets) dominates.

    Returns:
        The union of both sets, or None if either covers all targets.

    """
    if old is None or new is None:
        return None

    return old | new


@dataclass(frozen=True, slots=True)
class Mutation:
    """The state of a key to apply on a target.

    Attributes:
        key: The key.
        payload: DUMP payload, or None if the key does not exist (delete).
        expire_at_ms: Absolute expiry as unix time in ms, 0 for no expiry.

    """

    key: str
    payload: bytes | None
    expire_at_ms: int = 0

    @property
    def is_delete(self) -> bool:
        """True if the key has to be deleted on the target."""
        return self.payload is None


type FanOut = Callable[[Mutation, TargetSet], None]


class SnapshotFetcher:
    """Turns dirty keys into :class:`Mutation` snapshots and fans them out."""

    def __init__(
        self,
        client: Redis,
        dirty: CoalescingBuffer[str, TargetSet],
        fan_out: FanOut,
        backoff: ExponentialBackoff,
        batch_size: int,
    ) -> None:
        """Create a fetcher reading from *client*."""
        self._client = client
        self._dirty = dirty
        self._fan_out = fan_out
        self._backoff = backoff
        self._batch_size = batch_size
        self._busy = False
        self.snapshots = 0

    @property
    def idle(self) -> bool:
        """True if no dirty keys are pending or in flight."""
        return not self._busy and not len(self._dirty)

    async def snapshot(self, keys: list[str]) -> list[Mutation]:
        """Read DUMP payload and absolute expiry of *keys* atomically.

        Returns:
            One mutation per key, in the same order.

        """
        async with self._client.pipeline(transaction=True) as pipe:
            for key in keys:
                pipe.dump(key)
                pipe.pexpiretime(key)

            results = await pipe.execute()

        mutations: list[Mutation] = []

        for index, key in enumerate(keys):
            payload, expire_at = results[2 * index], results[2 * index + 1]

            if payload is None or expire_at == KEY_MISSING:
                mutations.append(Mutation(key, None))
            else:
                expiry = 0 if expire_at == NO_EXPIRY else int(expire_at)
                mutations.append(Mutation(key, payload, expiry))

        return mutations

    async def run(self) -> None:
        """Process dirty keys forever."""
        while True:
            batch = await self._dirty.drain(self._batch_size)
            self._busy = True
            mutations: list[Mutation] | None = None

            try:
                mutations = await self.snapshot([key for key, _ in batch])
            except (RedisError, OSError) as error:
                logger.warning(
                    "reading %d keys from local server failed: %s", len(batch), error
                )
            finally:
                # Also on cancellation: nothing read must get lost.
                if mutations is None:
                    self._dirty.requeue(batch)

                self._busy = False

            if mutations is None:
                await self._backoff.sleep()
                continue

            self._backoff.reset()
            self.snapshots += len(mutations)

            for (_, targets), mutation in zip(batch, mutations, strict=True):
                self._fan_out(mutation, targets)
