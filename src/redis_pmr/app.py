"""Wires listener, fetcher and targets together."""

import asyncio
import logging
from dataclasses import dataclass

from redis.exceptions import RedisError

from redis_pmr.backoff import ExponentialBackoff
from redis_pmr.buffer import CoalescingBuffer, TargetSet, merge_targets
from redis_pmr.clients import create_client, redact_url
from redis_pmr.config import NodeConfig, Settings
from redis_pmr.fetcher import Mutation, SnapshotFetcher
from redis_pmr.identity import LocalIdentity, remote_nodes
from redis_pmr.preflight import (
    PreflightError,
    ServerInfo,
    check_min_version,
    check_target,
    fetch_server_info,
    is_same_server,
    missing_notify_flags,
    read_notify_flags,
)
from redis_pmr.source import SourceListener
from redis_pmr.target import TargetReplicator, TargetState


logger = logging.getLogger(__name__)

IDLE_POLL_INTERVAL = 0.05


class Replicator:
    """Replicates keys of the local server to all other configured nodes."""

    def __init__(self, settings: Settings) -> None:
        """Build all components from *settings*."""
        self.settings = settings
        self.identity = LocalIdentity()
        self.dirty: CoalescingBuffer[str, TargetSet] = CoalescingBuffer(
            merge=merge_targets
        )
        self._data_client = create_client(settings.local, settings, retry=True)
        self.source = SourceListener(
            settings, self._data_client, self.dirty, self.identity
        )
        self.targets = [
            TargetReplicator(
                node, settings, self.identity, self.source.request_target_reconcile
            )
            for node in remote_nodes(settings.nodes, settings.node)
        ]
        self.fetcher = SnapshotFetcher(
            self._data_client,
            self.dirty,
            self._fan_out,
            ExponentialBackoff(settings.backoff),
            settings.replication,
        )

    def _fan_out(self, mutation: Mutation, targets: TargetSet) -> None:
        for target in self.targets:
            if targets is None or target.name in targets:
                target.submit(mutation)

    @property
    def idle(self) -> bool:
        """True if no work is pending anywhere."""
        return self.fetcher.idle and all(target.idle for target in self.targets)

    async def wait_idle(self, limit: float) -> bool:
        """Wait up to *limit* seconds until all pending work is applied.

        Returns:
            True if everything was applied, False on timeout.

        """
        try:
            async with asyncio.timeout(limit):
                # Idleness spans several components; polling is simplest and
                # only happens during shutdown.
                while not self.idle:  # ruff: ignore[async-busy-wait]
                    await asyncio.sleep(IDLE_POLL_INTERVAL)
        except TimeoutError:
            return False

        return True

    async def run(self, stop: asyncio.Event) -> None:
        """Replicate until *stop* is set, then drain pending work."""
        if self.settings.node is not None:
            logger.info("running as node '%s'", self.settings.node)

        try:
            async with asyncio.TaskGroup() as group:
                producers = [
                    group.create_task(self.source.run(), name="source"),
                    group.create_task(self.source.run_reconciles(), name="reconcile"),
                ]
                workers = [
                    group.create_task(self.fetcher.run(), name="fetcher"),
                    *(
                        group.create_task(target.run(), name=f"target:{target.name}")
                        for target in self.targets
                    ),
                ]

                if self.settings.replication.stats_interval > 0:
                    workers.append(group.create_task(self._log_stats(), name="stats"))

                await stop.wait()
                logger.info("shutting down")

                for task in producers:
                    task.cancel()

                timeout = self.settings.replication.shutdown_drain_timeout

                if not await self.wait_idle(timeout):
                    logger.warning(
                        "shutdown: %d dirty keys and %d mutations not replicated",
                        len(self.dirty),
                        sum(target.pending for target in self.targets),
                    )

                for task in workers:
                    task.cancel()
        finally:
            await self._data_client.aclose()

    def stats_line(self) -> str:
        """Summarize the counters of all components.

        Returns:
            A single human-readable log line.

        """
        targets = " ".join(
            f"{target.name}={target.state}/pending:{target.pending}"
            f"/applied:{target.applied}/errors:{target.errors}"
            f"/dropped:{target.dropped}"
            for target in self.targets
        )
        return (
            f"events={self.source.events} reconciled={self.source.reconciled} "
            f"dirty={len(self.dirty)} snapshots={self.fetcher.snapshots} "
            f"echoes={self.fetcher.echoes} {targets}"
        )

    async def _log_stats(self) -> None:
        while True:
            await asyncio.sleep(self.settings.replication.stats_interval)
            logger.info("stats: %s", self.stats_line())


@dataclass(frozen=True, slots=True)
class CheckResult:
    """Outcome of checking one server."""

    name: str
    ok: bool
    message: str


async def _check_local(settings: Settings) -> tuple[CheckResult, ServerInfo | None]:
    client = create_client(settings.local, settings, retry=False)

    try:
        info = await fetch_server_info(client)
        check_min_version(info, settings.replication.min_version)
        current = await read_notify_flags(client)
    except (RedisError, OSError, PreflightError) as error:
        return CheckResult("local", ok=False, message=str(error)), None
    finally:
        await client.aclose()

    message = f"Redis {info.version_str}, notify-keyspace-events '{current}'"

    if missing := missing_notify_flags(current):
        fixable = settings.local.manage_notify_config
        message += f", missing '{missing}'" + (" (will be set)" if fixable else "")
        return CheckResult("local", ok=fixable, message=message), info

    return CheckResult("local", ok=True, message=message), info


async def _check_node(
    settings: Settings, node: NodeConfig, local: ServerInfo | None
) -> CheckResult:
    name = node.name

    if name == settings.node:
        return CheckResult(name, ok=True, message="self (configured), skipped")

    client = create_client(node, settings, retry=False)

    try:
        info = await fetch_server_info(client)
    except (RedisError, OSError) as error:
        message = f"unreachable ({redact_url(node.url)}): {error}"
        return CheckResult(name, ok=False, message=message)
    finally:
        await client.aclose()

    if local is not None and is_same_server(local, info):
        return CheckResult(name, ok=True, message="self (detected by run_id), skipped")

    try:
        check_target(local, info, settings.replication.min_version)
    except PreflightError as error:
        return CheckResult(name, ok=False, message=str(error))

    return CheckResult(name, ok=True, message=f"Redis {info.version_str}")


async def run_check(settings: Settings) -> list[CheckResult]:
    """Check the local server and all nodes without changing anything.

    Returns:
        One result for the local server followed by one per node.

    """
    local_result, local = await _check_local(settings)
    node_results = await asyncio.gather(
        *(_check_node(settings, node, local) for node in settings.nodes)
    )
    return [local_result, *node_results]


__all__ = ["CheckResult", "Replicator", "TargetState", "run_check"]
