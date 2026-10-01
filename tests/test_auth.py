import asyncio
import logging
from pathlib import Path

import pytest
from pydantic import SecretStr
from redis.asyncio import Redis

from redis_pmr.app import Replicator, run_check
from redis_pmr.target import TargetState

from .conftest import (
    RedisServer,
    ServerFactory,
    eventually,
    for_local,
    running_mesh,
    target_states,
    write_settings,
)


ADMIN_PASSWORD = "admin-secret-1"
PMR_PASSWORD = "pmr-secret-2"
WRONG_PASSWORD = "wrong-secret-3"

# The ACL from the README, with the test pattern.
README_ACL = [
    "SETUSER",
    "pmr",
    "on",
    f">{PMR_PASSWORD}",
    "resetkeys",
    "~test:*",
    "resetchannels",
    "&__keyspace@0__:test:*",
    "-@all",
    "+info",
    "+ping",
    "+psubscribe",
    "+punsubscribe",
    "+dump",
    "+pexpiretime",
    "+scan",
    "+multi",
    "+exec",
    "+restore",
    "+del",
    "+config|get",
    "+config|set",
    "+client|setname",
    "+client|setinfo",
]


async def _get(client: Redis, key: str) -> bytes | None:
    return await client.get(key)


@pytest.fixture
async def secured(
    server_factory: ServerFactory,
) -> tuple[list[RedisServer], dict[str, Redis]]:
    servers = server_factory(admin_password=ADMIN_PASSWORD)
    admins = {server.name: server.client() for server in servers}
    yield servers, admins

    for admin in admins.values():
        await admin.aclose()


async def test_requirepass_via_auth_section(
    tmp_path: Path, secured: tuple[list[RedisServer], dict[str, Redis]]
) -> None:
    servers, admins = secured
    settings = write_settings(
        tmp_path / "pmr.toml", servers, f'\n[auth]\npassword = "{ADMIN_PASSWORD}"\n'
    )

    async with running_mesh(servers, settings):
        await admins["a"].set("test:pw", "ok", ex=100)
        await eventually(lambda: _get(admins["b"], "test:pw"), b"ok")
        await eventually(lambda: _get(admins["c"], "test:pw"), b"ok")


async def test_readme_acl_user(
    tmp_path: Path, secured: tuple[list[RedisServer], dict[str, Redis]]
) -> None:
    servers, admins = secured

    for admin in admins.values():
        await admin.execute_command("ACL", *README_ACL)

    settings = write_settings(
        tmp_path / "pmr.toml",
        servers,
        f'\n[auth]\nusername = "pmr"\npassword = "{PMR_PASSWORD}"\n',
    )

    async with running_mesh(servers, settings) as mesh:
        # notify-keyspace-events was set by the pmr user (CONFIG SET).
        config = await admins["b"].config_get("notify-keyspace-events")
        assert "K" in str(next(iter(config.values())))

        await admins["a"].set("test:acl", "v", px=100_000)
        await admins["b"].hset("test:acl-h", mapping={"f": "1"})
        await eventually(lambda: _get(admins["c"], "test:acl"), b"v")

        async def hash_on_a() -> dict:
            return await admins["a"].hgetall("test:acl-h")

        await eventually(hash_on_a, {b"f": b"1"})
        expected = await admins["a"].pexpiretime("test:acl")
        assert await admins["c"].pexpiretime("test:acl") == expected

        await admins["c"].delete("test:acl")
        await eventually(lambda: _get(admins["a"], "test:acl"), None)

        # Reconcile path: SCAN under the restricted user.
        await admins["a"].client_kill_filter(_type="pubsub")
        await admins["a"].set("test:acl-gap", "found")
        await eventually(lambda: _get(admins["b"], "test:acl-gap"), b"found", 10)

        async def reconciled() -> bool:
            return mesh["a"].source.reconciled > 0

        await eventually(reconciled, expected=True)


async def test_wrong_password_isolated_and_not_logged(
    tmp_path: Path,
    secured: tuple[list[RedisServer], dict[str, Redis]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    servers, _ = secured
    shared = write_settings(
        tmp_path / "pmr.toml", servers, f'\n[auth]\npassword = "{ADMIN_PASSWORD}"\n'
    )
    # Node b gets its own (wrong) password; node c uses the URL with a wrong
    # query password, which must not leak into logs either.
    nodes = [
        shared.nodes[0],
        shared.nodes[1].model_copy(update={"password": SecretStr(WRONG_PASSWORD)}),
        shared.nodes[2].model_copy(
            update={"url": f"{servers[2].url}?password={WRONG_PASSWORD}"}
        ),
    ]
    settings = for_local(shared.model_copy(update={"nodes": nodes}), servers[0])
    caplog.set_level(logging.DEBUG)

    stop = asyncio.Event()
    replicator = Replicator(settings)
    task = asyncio.create_task(replicator.run(stop))

    try:
        await eventually(
            lambda: target_states(replicator),
            {
                "a": TargetState.SELF,
                "b": TargetState.CONNECTING,
                "c": TargetState.CONNECTING,
            },
        )
        await asyncio.sleep(0.5)
        assert (await target_states(replicator))["b"] is TargetState.CONNECTING
    finally:
        stop.set()
        await task

    results = {result.name: result for result in await run_check(settings)}
    assert results["local"].ok
    assert not results["b"].ok
    assert not results["c"].ok

    output = caplog.text + "".join(result.message for result in results.values())
    assert WRONG_PASSWORD not in output
    assert ADMIN_PASSWORD not in output
    assert "invalid" in output.lower() or "WRONGPASS" in output


async def test_credentials_in_url(
    tmp_path: Path, secured: tuple[list[RedisServer], dict[str, Redis]]
) -> None:
    servers, _ = secured
    shared = write_settings(tmp_path / "pmr.toml", servers)
    nodes = [
        node.model_copy(update={"url": node.url.replace("//", f"//:{ADMIN_PASSWORD}@")})
        for node in shared.nodes
    ]
    local = shared.local.model_copy(
        update={"url": f"{servers[0].url}?password={ADMIN_PASSWORD}"}
    )
    settings = shared.model_copy(update={"nodes": nodes, "local": local})

    results = {result.name: result for result in await run_check(settings)}
    assert all(result.ok for result in results.values()), results
    assert "self (detected by run_id)" in results["a"].message
