"""Configuration models, loaded from a TOML file with environment overrides."""

from enum import StrEnum
from pathlib import Path
from typing import Self, override
from urllib.parse import parse_qs, urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)


REDIS_URL_SCHEMES = ("redis://", "rediss://", "unix://")


class LogLevel(StrEnum):
    """Supported log levels."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


def parse_version(version: str) -> tuple[int, ...]:
    """Parse a dotted version string like ``8.10.2`` into a tuple of ints.

    Non-numeric parts make ``int()`` raise a ValueError.

    Returns:
        The version parts as integers.

    """
    return tuple(int(part) for part in version.strip().split("."))


def _validate_redis_url(url: str) -> str:
    if not url.startswith(REDIS_URL_SCHEMES):
        msg = f"URL must start with one of {', '.join(REDIS_URL_SCHEMES)}"
        raise ValueError(msg)

    return url


def url_has_credentials(url: str) -> bool:
    """Check whether a Redis URL carries a username or password itself.

    Returns:
        True for ``user:pass@`` user info or a ``username``/``password``
        query parameter.

    """
    parts = urlsplit(url)

    if parts.username or parts.password:
        return True

    query = parse_qs(parts.query)
    return "username" in query or "password" in query


class _Model(BaseModel):
    # Validation errors must never echo URLs or passwords.
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)


class Credentials(_Model):
    """Username and password of a Redis server (ACL user or requirepass)."""

    username: str | None = None
    password: SecretStr | None = None

    def merged_with(self, fallback: Credentials) -> Credentials:
        """Fill unset fields from *fallback*.

        Returns:
            New credentials, field by field preferring this instance.

        """
        return Credentials(
            username=self.username or fallback.username,
            password=self.password or fallback.password,
        )


class Endpoint(Credentials):
    """A Redis server address with optional credentials."""

    url: str

    _check_url = field_validator("url")(_validate_redis_url)


class LocalConfig(Endpoint):
    """The local Redis server this process listens on (the replication source)."""

    url: str = "redis://localhost:6379/0"
    manage_notify_config: bool = True


class NodeConfig(Endpoint):
    """A Redis node participating in the replication mesh."""

    name: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")


class ReplicationConfig(_Model):
    """What to replicate and how."""

    patterns: list[str] = Field(min_length=1)
    batch_size: PositiveInt = 256
    max_pending_per_target: PositiveInt = 100_000
    reconcile_on_start: bool = False
    reconcile_on_reconnect: bool = True
    scan_count: PositiveInt = 1000
    shutdown_drain_timeout: NonNegativeFloat = 5.0
    stats_interval: NonNegativeFloat = 60.0
    min_server_version: str = "7.0"

    @field_validator("min_server_version")
    @classmethod
    def _check_version(cls, value: str) -> str:
        parse_version(value)
        return value

    @property
    def min_version(self) -> tuple[int, ...]:
        """The minimum server version as a tuple."""
        return parse_version(self.min_server_version)


class BackoffConfig(_Model):
    """Exponential reconnect backoff."""

    initial: PositiveFloat = 0.5
    maximum: PositiveFloat = 30.0
    factor: float = Field(default=2.0, ge=1.0)


class ConnectionConfig(_Model):
    """Socket-level settings applied to every Redis connection."""

    socket_timeout: PositiveFloat = 10.0
    connect_timeout: PositiveFloat = 5.0
    health_check_interval: NonNegativeFloat = 10.0
    keepalive_idle: PositiveInt = 10
    keepalive_interval: PositiveInt = 5
    keepalive_count: PositiveInt = 3


class Settings(BaseSettings):
    """Complete redis-pmr configuration.

    Values are taken (highest priority first) from explicit init arguments
    (CLI), ``PMR_*`` environment variables and the TOML file.
    """

    model_config = SettingsConfigDict(
        env_prefix="PMR_",
        env_nested_delimiter="__",
        extra="forbid",
        hide_input_in_errors=True,
    )

    node: str | None = None
    auth: Credentials = Credentials()
    local: LocalConfig = LocalConfig()
    replication: ReplicationConfig
    nodes: list[NodeConfig] = Field(min_length=1)
    backoff: BackoffConfig = BackoffConfig()
    connection: ConnectionConfig = ConnectionConfig()

    @model_validator(mode="after")
    def _check_nodes(self) -> Self:
        names = [node.name for node in self.nodes]

        if duplicates := sorted({name for name in names if names.count(name) > 1}):
            msg = f"duplicate node names: {', '.join(duplicates)}"
            raise ValueError(msg)

        if self.node is not None and self.node not in names:
            msg = f"node '{self.node}' is not defined in [[nodes]]"
            raise ValueError(msg)

        return self

    def credentials_for(self, endpoint: Endpoint) -> Credentials:
        """Resolve the credentials to use for *endpoint*.

        Credentials in the URL win completely (they are never mixed with
        configured ones). Otherwise the endpoint's own fields are used, each
        falling back to ``[auth]``.

        Returns:
            The credentials to pass to the client (possibly empty).

        """
        if url_has_credentials(endpoint.url):
            return Credentials()

        return endpoint.merged_with(self.auth)

    @classmethod
    @override
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Use init args, then environment, then the TOML file.

        Returns:
            The settings sources, highest priority first.

        """
        return (init_settings, env_settings, TomlConfigSettingsSource(settings_cls))


def load_settings(path: Path, node: str | None = None) -> Settings:
    """Load settings from the TOML file at *path*.

    Args:
        path: The TOML configuration file.
        node: Explicit name of the node this process runs on (overrides
            ``PMR_NODE`` and the file).

    An invalid configuration raises a ``pydantic.ValidationError``.

    Returns:
        The validated settings.

    """

    class _FileSettings(Settings):
        model_config = SettingsConfigDict(toml_file=path)

    overrides = {"node": node} if node is not None else {}
    return _FileSettings(**overrides)
