"""Server prerequisite checks."""

import logging
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from redis_pmr.config import format_version, parse_version


logger = logging.getLogger(__name__)

# K: keyspace channel; g: generic (del, expire, rename, ...); type classes.
REQUIRED_NOTIFY_FLAGS = "Kg$lshzt"
ALL_CLASSES_ALIAS = "A"
ALL_CLASSES = "g$lshztdxea"


class PreflightError(Exception):
    """A server does not meet the requirements."""


@dataclass(frozen=True, slots=True)
class ServerInfo:
    """Identity and version of a Redis server."""

    version: tuple[int, ...]
    run_id: str | None

    @property
    def version_str(self) -> str:
        """The version as dotted string."""
        return format_version(self.version)


def _text(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


async def fetch_server_info(client: Redis) -> ServerInfo:
    """Read version and run id from ``INFO server``.

    Returns:
        The server's version and run id.

    """
    info = await client.info("server")
    run_id = info.get("run_id")
    return ServerInfo(
        version=parse_version(_text(info["redis_version"])),
        run_id=_text(run_id) if run_id else None,
    )


def check_min_version(info: ServerInfo, minimum: tuple[int, ...]) -> None:
    """Raise if the server is older than *minimum*.

    Raises:
        PreflightError: If the server version is too old.

    """
    if info.version < minimum:
        msg = (
            f"Redis {info.version_str} is too old, at least "
            f"{format_version(minimum)} is required"
        )
        raise PreflightError(msg)


def is_same_server(first: ServerInfo, second: ServerInfo) -> bool:
    """Check whether two infos describe the same server process.

    Returns:
        True if both have the same (known) run id.

    """
    return first.run_id is not None and first.run_id == second.run_id


def is_rdb_compatible(source: ServerInfo, target: ServerInfo) -> bool:
    """Check whether DUMP payloads of *source* can be RESTOREd on *target*.

    RESTORE rejects payloads of a newer RDB format, so the target must not be
    older than the source (compared by major.minor).

    Returns:
        True if the target can restore the source's payloads.

    """
    return target.version[:2] >= source.version[:2]


def check_target(
    local: ServerInfo | None, target: ServerInfo, minimum: tuple[int, ...]
) -> None:
    """Verify that *target* can receive the payloads of *local*.

    Args:
        local: The local server, None if unknown (only the version is checked).
        target: The remote server.
        minimum: The minimum server version.

    Raises:
        PreflightError: If the target is too old.

    """
    check_min_version(target, minimum)

    if local is not None and not is_rdb_compatible(local, target):
        msg = (
            f"Redis {target.version_str} cannot RESTORE payloads of the local "
            f"Redis {local.version_str}; upgrade this node"
        )
        raise PreflightError(msg)


def expand_notify_flags(flags: str) -> set[str]:
    """Expand the ``A`` alias of a notify-keyspace-events value.

    Returns:
        The set of single-character flags.

    """
    expanded = set(flags.replace(ALL_CLASSES_ALIAS, ""))

    if ALL_CLASSES_ALIAS in flags:
        expanded |= set(ALL_CLASSES)

    return expanded


def missing_notify_flags(current: str) -> str:
    """Determine which required flags *current* lacks.

    Returns:
        The missing flags, empty if all are present.

    """
    present = expand_notify_flags(current)
    return "".join(flag for flag in REQUIRED_NOTIFY_FLAGS if flag not in present)


async def read_notify_flags(client: Redis) -> str:
    """Read the current notify-keyspace-events value.

    Returns:
        The configured flags (possibly empty).

    Raises:
        PreflightError: If the value cannot be read (e.g. missing ACL rights).

    """
    try:
        config = await client.config_get("notify-keyspace-events")
    except ResponseError as error:
        msg = f"cannot read notify-keyspace-events: {error}"
        raise PreflightError(msg) from error

    return _text(next(iter(config.values()), b""))


async def ensure_notify_flags(client: Redis, *, manage: bool) -> str:
    """Make sure keyspace notifications needed for replication are enabled.

    Args:
        client: Client of the server to check.
        manage: If True, add missing flags (keeping existing ones) via
            CONFIG SET, otherwise only check.

    Returns:
        The effective notify-keyspace-events value.

    Raises:
        PreflightError: If flags are missing and cannot or must not be set.

    """
    current = await read_notify_flags(client)

    if not (missing := missing_notify_flags(current)):
        return current

    if not manage:
        msg = (
            f"notify-keyspace-events is '{current}', missing flags '{missing}' "
            f"(manage_notify_config is disabled)"
        )
        raise PreflightError(msg)

    wanted = current + missing

    try:
        await client.config_set("notify-keyspace-events", wanted)
    except ResponseError as error:
        msg = f"cannot set notify-keyspace-events to '{wanted}': {error}"
        raise PreflightError(msg) from error

    logger.info("notify-keyspace-events changed from '%s' to '%s'", current, wanted)
    return wanted
