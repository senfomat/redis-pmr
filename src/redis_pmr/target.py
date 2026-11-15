"""Applies mutations to one remote node."""

import asyncio
import logging
from collections.abc import Callable
from enum import StrEnum

from redis import exceptions as redis_errors
from redis.asyncio import Redis
from redis.exceptions import RedisError

from redis_pmr.backoff import ExponentialBackoff
from redis_pmr.buffer import CoalescingBuffer
from redis_pmr.clients import create_client, redact_url
from redis_pmr.config import NodeConfig, Settings
from redis_pmr.fetcher import Mutation
from redis_pmr.identity import LocalIdentity
from redis_pmr.preflight import PreflightError, check_target, fetch_server_info


logger = logging.getLogger(__name__)


class TargetState(StrEnum):
    """Lifecycle state of a target."""

    CONNECTING = "connecting"
    ACTIVE = "active"
    INCOMPATIBLE = "incompatible"
    SELF = "self"


class TargetReplicator:
    """Owns the connection to one remote node and its pending mutations.

    Each target has its own buffer and task, so a slow or unreachable node
    never delays the local listener or any other target.
    """

    def __init__(
        self,
        node: NodeConfig,
        settings: Settings,
        identity: LocalIdentity,
        request_reconcile: Callable[[str], None],
    ) -> None:
        """Create a replicator for *node*."""
        self.node = node
        self._settings = settings
        self._identity = identity
        self._request_reconcile = request_reconcile
        self._buffer: CoalescingBuffer[str, Mutation] = CoalescingBuffer(
            max_size=settings.replication.max_pending_per_target
        )
        self._backoff = ExponentialBackoff(settings.backoff)
        self._needs_reconcile = False
        self._in_flight = 0
        self.state = TargetState.CONNECTING
        self.applied = 0
        self.errors = 0
        self.dropped = 0

    @property
    def name(self) -> str:
        """The node name."""
        return self.node.name

    @property
    def pending(self) -> int:
        """Number of mutations waiting or in flight."""
        return len(self._buffer) + self._in_flight

    @property
    def idle(self) -> bool:
        """True if nothing is left to apply (or the target is never written)."""
        return self.state is TargetState.SELF or self.pending == 0

    def submit(self, mutation: Mutation) -> None:
        """Queue *mutation*, replacing an older pending one for the same key."""
        if self.state is TargetState.SELF:
            return

        if self._buffer.put(mutation.key, mutation):
            return

        self.dropped += 1

        if not self._needs_reconcile:
            self._needs_reconcile = True
            logger.warning(
                "[%s] buffer full (%d keys), dropping mutations; "
                "a reconcile will run after the target is caught up",
                self.name,
                self._settings.replication.max_pending_per_target,
            )

    async def run(self) -> None:
        """Connect, verify and replicate until cancelled."""
        while self.state is not TargetState.SELF:
            client = create_client(self.node, self._settings, retry=False)

            try:
                await self._connect(client)

                if self.state is TargetState.SELF:
                    break

                await self._replicate(client)
            except PreflightError as error:
                self.state = TargetState.INCOMPATIBLE
                # Expected condition (old server version), a traceback adds nothing.
                logger.error("[%s] %s", self.name, error)  # ruff: ignore[error-instead-of-exception]
                await asyncio.sleep(self._settings.backoff.maximum)
            except (RedisError, OSError) as error:
                if self.state is TargetState.ACTIVE or self._backoff.attempt == 0:
                    logger.warning("[%s] connection problem: %s", self.name, error)
                else:
                    logger.debug("[%s] still unreachable: %s", self.name, error)

                self.state = TargetState.CONNECTING
                await self._backoff.sleep()
            finally:
                await client.aclose()

    async def _connect(self, client: Redis) -> None:
        local = await self._identity.wait()
        info = await fetch_server_info(client)

        if self._identity.is_local(info):
            self.state = TargetState.SELF
            self._buffer.clear()
            logger.info("[%s] is the local server, not replicating to it", self.name)
            return

        check_target(local, info, self._settings.replication.min_version)
        self.state = TargetState.ACTIVE
        self._backoff.reset()
        logger.info(
            "[%s] connected to %s (Redis %s)",
            self.name,
            redact_url(self.node.url),
            info.version_str,
        )

        if self._needs_reconcile:
            self._needs_reconcile = False
            self._request_reconcile(self.name)

    async def _replicate(self, client: Redis) -> None:
        batch_size = self._settings.replication.batch_size

        while True:
            batch = await self._buffer.drain(batch_size)
            self._in_flight = len(batch)

            try:
                await self._apply(client, [mutation for _, mutation in batch])
            except BaseException:
                self._buffer.requeue(batch)
                raise
            finally:
                self._in_flight = 0

            # Mutations were dropped while connected: the target is caught up
            # now, so a reconcile can restore the lost keys.
            if self._needs_reconcile and not len(self._buffer):
                self._needs_reconcile = False
                self._request_reconcile(self.name)

    async def _apply(self, client: Redis, mutations: list[Mutation]) -> None:
        replication = self._settings.replication

        # Two commands per mutation. A DEL raises a "del" event on the target,
        # which its own redis-pmr would replicate back (possibly deleting a
        # newer value); the marker set right before tells it to skip that echo.
        # RESTORE raises no replicated event, so it drops a stale marker instead.
        async with client.pipeline(transaction=False) as pipe:
            for mutation in mutations:
                marker = replication.echo_marker(mutation.key)

                if mutation.payload is None:
                    pipe.set(marker, b"1", px=replication.echo_marker_ttl_ms)
                    pipe.delete(mutation.key)
                else:
                    pipe.delete(marker)
                    pipe.restore(
                        mutation.key,
                        mutation.expire_at_ms,
                        mutation.payload,
                        replace=True,
                        absttl=True,
                    )

            results = await pipe.execute(raise_on_error=False)

        # Connection-level failures must requeue the whole batch.
        for result in results:
            if isinstance(
                result, (redis_errors.ConnectionError, redis_errors.TimeoutError)
            ):
                raise result

        for index, mutation in enumerate(mutations):
            pair = results[2 * index : 2 * index + 2]

            if error := next((r for r in pair if isinstance(r, Exception)), None):
                self.errors += 1
                logger.error(
                    "[%s] replicating key '%s' failed: %s",
                    self.name,
                    mutation.key,
                    error,
                )
            else:
                self.applied += 1
