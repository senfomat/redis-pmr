"""Listens for keyspace notifications on the local server."""

import asyncio
import logging

from redis.asyncio import Redis
from redis.asyncio.keyspace_notifications import AsyncKeyspaceNotifications
from redis.exceptions import RedisError

from redis_pmr.backoff import ExponentialBackoff
from redis_pmr.buffer import CoalescingBuffer, TargetSet, merge_targets
from redis_pmr.clients import client_db, create_client, redact_url
from redis_pmr.config import Settings
from redis_pmr.events import EventAction, classify
from redis_pmr.identity import LocalIdentity
from redis_pmr.preflight import (
    PreflightError,
    check_min_version,
    ensure_notify_flags,
    fetch_server_info,
)


logger = logging.getLogger(__name__)


class SourceListener:
    """Marks keys as dirty when keyspace notifications arrive.

    Pub/Sub is fire-and-forget, so every reconnect means events may have been
    lost. The listener therefore never relies on redis-py's silent reconnect:
    it builds a fresh connection, re-runs the preflight, re-subscribes and
    optionally reconciles the configured patterns via SCAN.
    """

    def __init__(
        self,
        settings: Settings,
        data_client: Redis,
        dirty: CoalescingBuffer[str, TargetSet],
        identity: LocalIdentity,
    ) -> None:
        """Create a listener.

        Args:
            settings: The configuration.
            data_client: Client used for SCAN during reconciles.
            dirty: Receives the keys to replicate.
            identity: Updated with the local server's info on every connect.

        """
        self._settings = settings
        self._data_client = data_client
        self._dirty = dirty
        self._identity = identity
        self._backoff = ExponentialBackoff(settings.backoff)
        self._reconcile_requests: asyncio.Queue[TargetSet] = asyncio.Queue()
        self._connected_once = False
        self.events = 0
        self.reconciled = 0

    def request_reconcile(self, targets: TargetSet = None) -> None:
        """Ask for the pattern keys to be pushed again to *targets* (None = all)."""
        self._reconcile_requests.put_nowait(targets)

    def request_target_reconcile(self, target: str) -> None:
        """Ask for the pattern keys to be pushed again to one target."""
        self.request_reconcile(frozenset({target}))

    async def run(self) -> None:
        """Listen until cancelled, reconnecting on failures.

        Raises:
            PreflightError: If the local server fails the preflight before the
                first successful connect (later failures are retried).

        """
        while True:
            client = create_client(self._settings.local, self._settings, retry=False)

            try:
                await self._listen(client)
            except PreflightError as error:
                if not self._connected_once:
                    raise

                # Expected condition (e.g. CONFIG locked), a traceback adds nothing.
                logger.error("local server: %s", error)  # ruff: ignore[error-instead-of-exception]
                await asyncio.sleep(self._settings.backoff.maximum)
            except (RedisError, OSError) as error:
                if self._backoff.attempt == 0:
                    logger.warning("local server connection problem: %s", error)
                else:
                    logger.debug("local server still unreachable: %s", error)

                await self._backoff.sleep()
            finally:
                await client.aclose()

    async def _listen(self, client: Redis) -> None:
        replication = self._settings.replication
        info = await fetch_server_info(client)
        check_min_version(info, replication.min_version)
        flags = await ensure_notify_flags(
            client, manage=self._settings.local.manage_notify_config
        )
        db = client_db(client)

        async with AsyncKeyspaceNotifications(client) as notifications:
            for pattern in replication.patterns:
                await notifications.subscribe_keyspace(pattern, db=db)

            self._identity.update(info)
            self._backoff.reset()
            logger.info(
                "listening on %s (Redis %s, db %d, notify '%s') for %s",
                redact_url(self._settings.local.url),
                info.version_str,
                db,
                flags,
                ", ".join(replication.patterns),
            )

            # Subscribed first, so nothing is missed between scan and events.
            if self._connected_once:
                if replication.reconcile_on_reconnect:
                    self.request_reconcile()
                else:
                    logger.warning("reconnected; events in between are lost")

            elif replication.reconcile_on_start:
                self.request_reconcile()

            self._connected_once = True

            async for notification in notifications.listen():
                if classify(notification.event_type) is EventAction.SYNC:
                    self.events += 1
                    self._dirty.put(notification.key, None)

    async def run_reconciles(self) -> None:
        """Serve reconcile requests until cancelled."""
        while True:
            targets = await self._reconcile_requests.get()

            while not self._reconcile_requests.empty():
                targets = merge_targets(targets, self._reconcile_requests.get_nowait())

            try:
                count = await self.reconcile(targets)
            except (RedisError, OSError) as error:
                logger.warning("reconcile failed, retrying: %s", error)
                self._reconcile_requests.put_nowait(targets)
                await asyncio.sleep(self._settings.backoff.maximum)
                continue

            self.reconciled += count
            logger.info(
                "reconcile queued %d keys for %s",
                count,
                "all targets" if targets is None else ", ".join(sorted(targets)),
            )

    async def reconcile(self, targets: TargetSet) -> int:
        """Mark every key matching the patterns as dirty for *targets*.

        Returns:
            The number of keys marked.

        """
        count = 0

        for pattern in self._settings.replication.patterns:
            async for key in self._data_client.scan_iter(
                match=pattern, count=self._settings.replication.scan_count
            ):
                self._dirty.put(key.decode("utf-8", errors="replace"), targets)
                count += 1

        return count
