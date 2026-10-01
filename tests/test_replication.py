import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest
from redis.asyncio import Redis

from redis_pmr.app import run_check
from redis_pmr.config import Settings
from redis_pmr.target import TargetState

from .conftest import (
    Mesh,
    RedisServer,
    eventually,
    for_local,
    running_mesh,
    target_states,
    write_settings,
)


@pytest.fixture
def shared_settings(tmp_path: Path, servers: list[RedisServer]) -> Settings:
    return write_settings(tmp_path / "pmr.toml", servers)


@pytest.fixture
async def mesh(
    servers: list[RedisServer], shared_settings: Settings
) -> AsyncIterator[Mesh]:
    async with running_mesh(servers, shared_settings) as replicators:
        yield replicators


def get_probe(client: Redis, key: str) -> Callable[[], Awaitable[bytes | None]]:
    async def probe() -> bytes | None:
        return await client.get(key)

    return probe


async def test_self_detection_by_run_id(mesh: Mesh) -> None:
    for name, replicator in mesh.items():
        states = await target_states(replicator)
        assert states[name] is TargetState.SELF
        assert all(
            state is TargetState.ACTIVE
            for other, state in states.items()
            if other != name
        )


@pytest.mark.usefixtures("mesh")
async def test_string_with_ttl(clients: dict[str, Redis]) -> None:
    await clients["a"].set("test:1", "v1", px=100_000)

    for name in ("b", "c"):
        await eventually(get_probe(clients[name], "test:1"), b"v1")

    expected = await clients["a"].pexpiretime("test:1")
    assert await clients["b"].pexpiretime("test:1") == expected
    assert await clients["c"].pexpiretime("test:1") == expected

    await clients["b"].persist("test:1")

    async def ttl_on_c() -> int:
        return await clients["c"].pexpiretime("test:1")

    await eventually(ttl_on_c, -1)


@pytest.mark.usefixtures("mesh")
async def test_data_types_multi_master(clients: dict[str, Redis]) -> None:
    await clients["b"].hset("test:h", mapping={"f1": "1", "f2": "2"})
    await clients["b"].rpush("test:l", "x", "y", "z")
    await clients["c"].zadd("test:z", {"m1": 1.0, "m2": 2.0})

    async def hash_on_a() -> dict:
        return await clients["a"].hgetall("test:h")

    async def list_on_a() -> list:
        return await clients["a"].lrange("test:l", 0, -1)

    async def zset_on_b() -> list:
        return await clients["b"].zrange("test:z", 0, -1, withscores=True)

    await eventually(hash_on_a, {b"f1": b"1", b"f2": b"2"})
    await eventually(list_on_a, [b"x", b"y", b"z"])
    await eventually(zset_on_b, [(b"m1", 1.0), (b"m2", 2.0)])

    for _ in range(50):
        await clients["a"].hincrby("test:h", "f1", 1)

    async def counter_on_c() -> bytes | None:
        return await clients["c"].hget("test:h", "f1")

    await eventually(counter_on_c, b"51")


@pytest.mark.usefixtures("mesh")
async def test_delete_and_rename(clients: dict[str, Redis]) -> None:
    await clients["a"].set("test:del", "x")
    await clients["a"].set("test:old", "y")
    await eventually(get_probe(clients["c"], "test:old"), b"y")

    await clients["b"].delete("test:del")
    await clients["c"].rename("test:old", "test:new")

    await eventually(get_probe(clients["a"], "test:del"), None)
    await eventually(get_probe(clients["a"], "test:old"), None)
    await eventually(get_probe(clients["a"], "test:new"), b"y")
    await eventually(get_probe(clients["b"], "test:new"), b"y")


@pytest.mark.usefixtures("mesh")
async def test_only_matching_keys(clients: dict[str, Redis]) -> None:
    await clients["a"].set("private:1", "nope")
    await clients["a"].set("test:marker", "yes")
    await eventually(get_probe(clients["b"], "test:marker"), b"yes")
    assert await clients["b"].get("private:1") is None


async def test_no_ping_pong(mesh: Mesh, clients: dict[str, Redis]) -> None:
    await clients["a"].set("test:pp", "1")
    await eventually(get_probe(clients["b"], "test:pp"), b"1")
    await eventually(get_probe(clients["c"], "test:pp"), b"1")
    await asyncio.sleep(0.3)

    events = {name: replicator.source.events for name, replicator in mesh.items()}
    assert events["b"] == 0
    assert events["c"] == 0

    await asyncio.sleep(0.5)
    assert {name: r.source.events for name, r in mesh.items()} == events


@pytest.mark.usefixtures("mesh")
async def test_target_outage_does_not_block_others(
    servers: list[RedisServer], clients: dict[str, Redis]
) -> None:
    server_c = servers[2]
    server_c.stop()

    await clients["a"].set("test:outage", "during")
    await eventually(get_probe(clients["b"], "test:outage"), b"during")

    server_c.start()
    client_c = server_c.client()

    try:
        await eventually(get_probe(client_c, "test:outage"), b"during", timeout=10)
    finally:
        await client_c.aclose()


async def test_local_restart_restores_notify_flags(
    mesh: Mesh, servers: list[RedisServer], clients: dict[str, Redis]
) -> None:
    server_a = servers[0]
    server_a.stop()
    server_a.start()

    await eventually(
        lambda: target_states(mesh["a"]),
        {"a": TargetState.SELF, "b": TargetState.ACTIVE, "c": TargetState.ACTIVE},
        timeout=10,
    )
    client_a = server_a.client()

    async def flags() -> bool:
        config = await client_a.config_get("notify-keyspace-events")
        value = next(iter(config.values()))
        return "K" in (value.decode() if isinstance(value, bytes) else value)

    try:
        await eventually(flags, expected=True, timeout=10)
        await client_a.set("test:after-restart", "ok")
        await eventually(
            get_probe(clients["b"], "test:after-restart"), b"ok", timeout=10
        )
    finally:
        await client_a.aclose()


async def test_reconcile_after_pubsub_drop(
    mesh: Mesh, clients: dict[str, Redis]
) -> None:
    # Write a key while the listener is disconnected; only the reconcile
    # after the reconnect can pick it up.
    await clients["a"].client_kill_filter(_type="pubsub")
    await clients["a"].set("test:gap", "found")
    await eventually(get_probe(clients["b"], "test:gap"), b"found", timeout=10)

    async def reconciled() -> bool:
        return mesh["a"].source.reconciled > 0

    await eventually(reconciled, expected=True)


async def test_check(servers: list[RedisServer], shared_settings: Settings) -> None:
    results = await run_check(for_local(shared_settings, servers[1]))
    by_name = {result.name: result for result in results}
    assert by_name["local"].ok
    assert "self (detected by run_id)" in by_name["b"].message
    assert by_name["a"].ok
    assert by_name["c"].ok
