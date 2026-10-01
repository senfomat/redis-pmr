"""Factory for consistently configured async Redis clients."""

import socket
from urllib.parse import parse_qsl, urlencode, urlsplit

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff, NoBackoff

from redis_pmr.config import ConnectionConfig, Endpoint, Settings


CLIENT_NAME = "redis-pmr"
REDACTED = "***"
SECRET_QUERY_PARAMS = frozenset({"username", "password"})


def keepalive_options(config: ConnectionConfig) -> dict[int, int]:
    """Build TCP keepalive socket options for the current platform.

    Aggressive keepalives detect half-open connections (e.g. after a network
    partition) within seconds instead of the OS default of hours.

    Returns:
        Socket option numbers mapped to their values.

    """
    options: dict[int, int] = {}

    # Linux names it TCP_KEEPIDLE, macOS TCP_KEEPALIVE.
    if (idle := getattr(socket, "TCP_KEEPIDLE", None)) is None:
        idle = getattr(socket, "TCP_KEEPALIVE", None)

    if idle is not None:
        options[idle] = config.keepalive_idle

    if (interval := getattr(socket, "TCP_KEEPINTVL", None)) is not None:
        options[interval] = config.keepalive_interval

    if (count := getattr(socket, "TCP_KEEPCNT", None)) is not None:
        options[count] = config.keepalive_count

    return options


def create_client(endpoint: Endpoint, settings: Settings, *, retry: bool) -> Redis:
    """Create an async client for *endpoint*.

    Args:
        endpoint: Server URL (the database is taken from the URL) and
            optional credentials.
        settings: Provides socket settings and ``[auth]`` defaults.
        retry: If False, connection errors surface immediately so the caller's
            own reconnect loop notices them. If True, transient errors are
            retried a few times transparently.

    Returns:
        The (not yet connected) client.

    """
    config = settings.connection
    credentials = settings.credentials_for(endpoint)
    policy = (
        Retry(ExponentialBackoff(cap=1.0, base=0.05), 3)
        if retry
        else Retry(NoBackoff(), 0)
    )
    # Unix domain socket connections reject the TCP keepalive arguments.
    tcp_options = (
        {}
        if endpoint.url.startswith("unix://")
        else {
            "socket_keepalive": True,
            "socket_keepalive_options": keepalive_options(config),
        }
    )
    return Redis.from_url(
        endpoint.url,
        decode_responses=False,
        client_name=CLIENT_NAME,
        username=credentials.username,
        password=(
            credentials.password.get_secret_value() if credentials.password else None
        ),
        socket_timeout=config.socket_timeout,
        socket_connect_timeout=config.connect_timeout,
        health_check_interval=config.health_check_interval,
        retry=policy,
        **tcp_options,
    )


def client_db(client: Redis) -> int:
    """Look up the database a client is bound to.

    Returns:
        The database number.

    """
    return int(client.connection_pool.connection_kwargs.get("db", 0) or 0)


def redact_url(url: str) -> str:
    """Remove credentials from a Redis URL for logging.

    Returns:
        The URL with user info and ``username``/``password`` query values
        replaced by ``***``.

    """
    parts = urlsplit(url)
    netloc = parts.netloc

    if "@" in netloc:
        netloc = f"***@{netloc.rpartition('@')[2]}"

    query = urlencode(
        [
            (name, REDACTED if name.lower() in SECRET_QUERY_PARAMS else value)
            for name, value in parse_qsl(parts.query, keep_blank_values=True)
        ],
        safe="*",
    )
    # Built by hand: urlunsplit drops the "//" of unix:///path URLs.
    redacted = f"{parts.scheme}://{netloc}{parts.path}"

    if query:
        redacted += f"?{query}"

    return redacted
