"""Server prerequisite checks."""

import logging
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from redis_pmr.config import parse_version


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
        return ".".join(map(str, self.version))


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
            f"{'.'.join(map(str, minimum))} is required"
        )
        raise PreflightError(msg)


def is_rdb_compatible(source: ServerInfo, target: ServerInfo) -> bool:
    """Check whether DUMP payloads of *source* can be RESTOREd on *target*.

    RESTORE rejects payloads of a newer RDB format, so the target must not be
    older than the source (compared by major.minor).

    Returns:
        True if the target can restore the source's payloads.

    """
    return target.version[:2] >= source.version[:2]


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
    try:
        config = await client.config_get("notify-keyspace-events")
    except ResponseError as error:
        msg = f"cannot read notify-keyspace-events: {error}"
        raise PreflightError(msg) from error

    current = _text(next(iter(config.values()), b""))

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
