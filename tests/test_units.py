import asyncio
import operator
from pathlib import Path

import pytest
from pydantic import ValidationError

from redis_pmr.backoff import ExponentialBackoff
from redis_pmr.buffer import CoalescingBuffer, merge_targets
from redis_pmr.clients import client_db, create_client, redact_url
from redis_pmr.config import (
    BackoffConfig,
    Credentials,
    LocalConfig,
    NodeConfig,
    ReplicationConfig,
    Settings,
    format_version,
    load_settings,
    parse_version,
    url_has_credentials,
)
from redis_pmr.events import EventAction, classify
from redis_pmr.fetcher import Mutation
from redis_pmr.identity import LocalIdentity, remote_nodes
from redis_pmr.preflight import (
    PreflightError,
    ServerInfo,
    check_min_version,
    check_target,
    expand_notify_flags,
    is_rdb_compatible,
    is_same_server,
    missing_notify_flags,
)
from redis_pmr.target import TargetReplicator, TargetState


CONFIG = """
[replication]
patterns = ["test:*"]

[[nodes]]
name = "a"
url = "redis://a:6379/0"

[[nodes]]
name = "b"
url = "redis://b:6379/0"
"""


@pytest.mark.parametrize(
    ("event", "action"),
    [
        ("set", EventAction.SYNC),
        ("del", EventAction.SYNC),
        ("expire", EventAction.SYNC),
        ("hset", EventAction.SYNC),
        ("rename_from", EventAction.SYNC),
        ("some_future_event", EventAction.SYNC),
        ("restore", EventAction.IGNORE),
        ("expired", EventAction.IGNORE),
        ("evicted", EventAction.IGNORE),
        ("new", EventAction.IGNORE),
    ],
)
def test_classify(event: str, action: EventAction) -> None:
    assert classify(event) is action


@pytest.mark.parametrize(
    ("current", "missing"),
    [
        ("", "Kg$lshzt"),
        ("KEA", ""),
        ("AK", ""),
        ("Ex", "Kg$lshzt"),
        ("Kg$", "lshzt"),
        ("g$lshztKE", ""),
    ],
)
def test_missing_notify_flags(current: str, missing: str) -> None:
    assert missing_notify_flags(current) == missing


def test_expand_notify_flags() -> None:
    assert expand_notify_flags("AK") >= set("Kg$lshztdxea")


def test_versions() -> None:
    assert parse_version("8.10.2") == (8, 10, 2)
    old = ServerInfo((6, 2, 14), "x")
    new = ServerInfo((8, 10, 2), "y")

    with pytest.raises(PreflightError):
        check_min_version(old, (7, 0))

    check_min_version(new, (7, 0))
    assert is_rdb_compatible(old, new)
    assert not is_rdb_compatible(new, old)
    assert is_rdb_compatible(ServerInfo((8, 10, 0), None), ServerInfo((8, 10, 9), None))
    assert format_version((8, 10, 2)) == "8.10.2"


def test_check_target() -> None:
    old = ServerInfo((7, 2, 0), "x")
    new = ServerInfo((8, 0, 0), "y")
    check_target(old, new, (7, 0))
    check_target(None, old, (7, 0))

    with pytest.raises(PreflightError, match="cannot RESTORE"):
        check_target(new, old, (7, 0))

    with pytest.raises(PreflightError, match="too old"):
        check_target(None, old, (8, 0))

    assert is_same_server(old, ServerInfo((7, 2, 0), "x"))
    assert not is_same_server(ServerInfo((8, 0), None), ServerInfo((8, 0), None))


def test_identity() -> None:
    identity = LocalIdentity()
    assert not identity.is_local(ServerInfo((8, 0), "abc"))
    identity.update(ServerInfo((8, 0), "abc"))
    assert identity.is_local(ServerInfo((8, 0), "abc"))
    assert not identity.is_local(ServerInfo((8, 0), "other"))
    assert not identity.is_local(ServerInfo((8, 0), None))


def test_merge_targets() -> None:
    assert merge_targets(None, frozenset({"a"})) is None
    assert merge_targets(frozenset({"a"}), frozenset({"b"})) == {"a", "b"}


async def test_buffer_coalesces_and_moves_to_end() -> None:
    buffer: CoalescingBuffer[str, int] = CoalescingBuffer()
    buffer.put("a", 1)
    buffer.put("b", 1)
    buffer.put("a", 2)
    assert await buffer.drain(10) == [("b", 1), ("a", 2)]
    assert len(buffer) == 0


async def test_buffer_merge_and_overflow() -> None:
    buffer: CoalescingBuffer[str, int] = CoalescingBuffer(
        max_size=2, merge=operator.add
    )
    assert buffer.put("a", 1)
    assert buffer.put("b", 1)
    assert not buffer.put("c", 1)
    assert buffer.put("a", 5)
    assert buffer.take(10) == [("b", 1), ("a", 6)]


async def test_buffer_requeue_keeps_newer_value() -> None:
    buffer: CoalescingBuffer[str, int] = CoalescingBuffer()
    buffer.put("a", 1)
    buffer.put("b", 1)
    batch = buffer.take(2)
    buffer.put("b", 2)
    buffer.put("c", 1)
    buffer.requeue(batch)
    assert buffer.take(10) == [("a", 1), ("b", 2), ("c", 1)]


async def test_buffer_drain_waits() -> None:
    buffer: CoalescingBuffer[str, int] = CoalescingBuffer()
    task = asyncio.create_task(buffer.drain(1))
    await asyncio.sleep(0.01)
    assert not task.done()
    buffer.put("x", 1)
    assert await task == [("x", 1)]


def test_backoff_grows_and_resets() -> None:
    backoff = ExponentialBackoff(BackoffConfig(initial=1.0, maximum=4.0, factor=2.0))
    delays = [backoff.next_delay() for _ in range(5)]
    assert 0.5 <= delays[0] <= 1.0
    assert all(delay <= 4.0 for delay in delays)
    assert delays[-1] >= 2.0
    backoff.reset()
    assert backoff.next_delay() <= 1.0


async def test_overflow_reconcile_while_connected(tmp_path: Path) -> None:
    path = tmp_path / "pmr.toml"
    path.write_text(
        CONFIG.replace('["test:*"]', '["test:*"]\nmax_pending_per_target = 2')
    )
    settings = load_settings(path)
    requested: list[str] = []
    reconciled = asyncio.Event()

    def request(name: str) -> None:
        requested.append(name)
        reconciled.set()

    target = TargetReplicator(settings.nodes[1], settings, LocalIdentity(), request)
    target.state = TargetState.ACTIVE

    for key in ("test:1", "test:2", "test:3"):
        target.submit(Mutation(key, b"x"))

    assert target.dropped == 1

    async def apply(_client: object, _mutations: list[Mutation]) -> None:
        await asyncio.sleep(0)

    target._apply = apply  # type: ignore[method-assign]  # ruff: ignore[private-member-access]
    task = asyncio.create_task(target._replicate(None))  # type: ignore[arg-type]  # ruff: ignore[private-member-access]

    async with asyncio.timeout(1):
        await reconciled.wait()

    task.cancel()
    assert requested == ["b"]
    assert target.pending == 0


def test_load_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "pmr.toml"
    path.write_text(CONFIG)
    settings = load_settings(path)
    assert settings.node is None
    assert settings.local.url == "redis://localhost:6379/0"
    assert [node.name for node in remote_nodes(settings.nodes, "a")] == ["b"]

    monkeypatch.setenv("PMR_NODE", "b")
    monkeypatch.setenv("PMR_LOCAL__URL", "redis://other:6380/1")
    settings = load_settings(path)
    assert settings.node == "b"
    assert settings.local.url == "redis://other:6380/1"
    assert load_settings(path, node="a").node == "a"


def test_settings_validation(tmp_path: Path) -> None:
    path = tmp_path / "pmr.toml"
    path.write_text(CONFIG)

    with pytest.raises(ValidationError, match="not defined"):
        load_settings(path, node="zzz")

    path.write_text(CONFIG + '\n[[nodes]]\nname = "a"\nurl = "redis://x:1/0"\n')

    with pytest.raises(ValidationError, match="duplicate"):
        load_settings(path)

    path.write_text(CONFIG.replace("redis://a", "http://a"))

    with pytest.raises(ValidationError, match="URL must start"):
        load_settings(path)

    for patterns in ('["*"]', '["pmr:*"]', '["pmr:echo:x:*"]'):
        path.write_text(CONFIG.replace('["test:*"]', patterns))

        with pytest.raises(ValidationError, match="echo_marker_prefix"):
            load_settings(path)

    path.write_text(CONFIG.replace('["test:*"]', '["session:*", "lock:?"]'))
    settings = load_settings(path)
    assert settings.replication.echo_marker("session:1") == "pmr:echo:session:1"
    assert settings.replication.echo_marker_ttl_ms == 10_000


@pytest.mark.parametrize(
    ("url", "db"),
    [
        ("unix:///run/redis/redis.sock?db=3", 3),
        ("redis://localhost:6379/2", 2),
        ("rediss://user:pw@host:6380/0", 0),
    ],
)
async def test_create_client_builds_connections(url: str, db: int) -> None:
    # Unix socket connections reject TCP keepalive arguments; constructing a
    # connection (without connecting) catches that.
    settings = _settings(url)
    client = create_client(settings.local, settings, retry=False)
    client.connection_pool.make_connection()
    assert client_db(client) == db
    await client.aclose()


def _settings(local_url: str = "redis://localhost/0", **extra: object) -> Settings:
    return Settings(
        local=LocalConfig(url=local_url),
        replication=ReplicationConfig(patterns=["x:*"]),
        nodes=[NodeConfig(name="a", url="redis://a/0")],
        **extra,
    )


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("redis://host:6379/0", False),
        ("redis://:pw@host:6379/0", True),
        ("redis://user@host:6379/0", True),
        ("unix:///run/redis.sock?db=0&password=pw", True),
        ("unix:///run/redis.sock?db=0", False),
    ],
)
def test_url_has_credentials(url: str, *, expected: bool) -> None:
    assert url_has_credentials(url) is expected


def test_credentials_resolution() -> None:
    settings = _settings(auth=Credentials(username="pmr", password="shared"))
    own = NodeConfig(name="b", url="redis://b/0", password="own")
    in_url = NodeConfig(name="c", url="redis://u:inurl@c/0", password="ignored")

    default = settings.credentials_for(NodeConfig(name="a", url="redis://a/0"))
    assert default.username == "pmr"
    assert default.password is not None
    assert default.password.get_secret_value() == "shared"

    # Field-wise: own password, username still from [auth].
    resolved = settings.credentials_for(own)
    assert resolved.username == "pmr"
    assert resolved.password is not None
    assert resolved.password.get_secret_value() == "own"

    assert settings.credentials_for(in_url) == Credentials()


async def test_create_client_passes_credentials() -> None:
    settings = _settings(auth=Credentials(username="pmr", password="shared"))
    configured = create_client(settings.local, settings, retry=False)
    from_url = create_client(
        NodeConfig(name="c", url="redis://u:inurl@c/0"), settings, retry=False
    )

    assert configured.connection_pool.connection_kwargs["username"] == "pmr"
    assert configured.connection_pool.connection_kwargs["password"] == "shared"
    assert from_url.connection_pool.connection_kwargs["username"] == "u"
    assert from_url.connection_pool.connection_kwargs["password"] == "inurl"

    await configured.aclose()
    await from_url.aclose()


def test_secrets_are_not_exposed(tmp_path: Path) -> None:
    settings = _settings(auth=Credentials(password="topsecret"))
    assert "topsecret" not in repr(settings)
    assert "topsecret" not in str(settings.model_dump())

    path = tmp_path / "pmr.toml"
    path.write_text(
        CONFIG.replace("redis://a:6379", "http://:topsecret@a:6379")
        + '\n[auth]\npassword = "topsecret"\n'
    )

    with pytest.raises(ValidationError) as error:
        load_settings(path)

    assert "topsecret" not in str(error.value)


def test_env_password(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "pmr.toml"
    path.write_text(CONFIG)
    monkeypatch.setenv("PMR_AUTH__PASSWORD", "fromenv")
    monkeypatch.setenv("PMR_LOCAL__USERNAME", "localuser")
    settings = load_settings(path)
    local = settings.credentials_for(settings.local)
    assert local.username == "localuser"
    assert local.password is not None
    assert local.password.get_secret_value() == "fromenv"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("rediss://user:pw@host:6380/0", "rediss://***@host:6380/0"),
        ("redis://:pw@host:6379/0", "redis://***@host:6379/0"),
        ("unix:///run/redis.sock", "unix:///run/redis.sock"),
        (
            "unix://pmr@/run/redis.sock?db=0&password=s3cret",
            "unix://***@/run/redis.sock?db=0&password=***",
        ),
        (
            "redis://host:6379/0?username=u&password=s3cret",
            "redis://host:6379/0?username=***&password=***",
        ),
    ],
)
def test_redact_url(url: str, expected: str) -> None:
    assert redact_url(url) == expected
