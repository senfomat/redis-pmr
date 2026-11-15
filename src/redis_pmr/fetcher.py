"""Reads the current state of dirty keys from the local server."""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError

from redis_pmr.backoff import ExponentialBackoff
from redis_pmr.buffer import CoalescingBuffer, TargetSet
from redis_pmr.config import ReplicationConfig


logger = logging.getLogger(__name__)

NO_EXPIRY = -1
KEY_MISSING = -2


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
        replication: ReplicationConfig,
    ) -> None:
        """Create a fetcher reading from *client*.

        Args:
            client: Client of the local server.
            dirty: The keys to snapshot.
            fan_out: Receives every mutation with its targets.
            backoff: Delay between failed reads.
            replication: Provides the batch size and the echo marker names.

        """
        self._client = client
        self._dirty = dirty
        self._fan_out = fan_out
        self._backoff = backoff
        self._batch_size = replication.batch_size
        self._echo_marker = replication.echo_marker
        self._busy = False
        self.snapshots = 0
        self.echoes = 0

    @property
    def idle(self) -> bool:
        """True if no dirty keys are pending or in flight."""
        return not self._busy and not len(self._dirty)

    async def snapshot(self, keys: list[str]) -> list[Mutation | None]:
        """Read DUMP payload and absolute expiry of *keys* atomically.

        Every snapshot also consumes the key's echo marker, so a stale marker
        can never suppress a later, genuine deletion.

        Returns:
            One mutation per key, in the same order; None for a deletion that
            another redis-pmr wrote (replicating it back would be an echo).

        """
        async with self._client.pipeline(transaction=True) as pipe:
            for key in keys:
                pipe.dump(key)
                pipe.pexpiretime(key)
                pipe.getdel(self._echo_marker(key))

            results = await pipe.execute()

        mutations: list[Mutation | None] = []

        for index, key in enumerate(keys):
            payload, expire_at, marker = results[3 * index : 3 * index + 3]

            if payload is None or expire_at == KEY_MISSING:
                mutations.append(None if marker is not None else Mutation(key, None))
            else:
                expiry = 0 if expire_at == NO_EXPIRY else int(expire_at)
                mutations.append(Mutation(key, payload, expiry))

        return mutations

    async def run(self) -> None:
        """Process dirty keys forever."""
        while True:
            batch = await self._dirty.drain(self._batch_size)
            self._busy = True
            mutations: list[Mutation | None] | None = None

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
                if mutation is None:
                    self.echoes += 1
                else:
                    self._fan_out(mutation, targets)
