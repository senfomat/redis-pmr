import asyncio
import shutil
import socket
import subprocess
import time
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
)
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import redis
from redis.asyncio import Redis

from redis_pmr.app import Replicator
from redis_pmr.config import Settings, load_settings
from redis_pmr.target import TargetState


REDIS_SERVER = shutil.which("redis-server")

type Mesh = dict[str, Replicator]
type ServerFactory = Callable[..., list[RedisServer]]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RedisServer:
    """A throw-away redis-server process that can be stopped and restarted."""

    def __init__(
        self, name: str, workdir: Path, admin_password: str | None = None
    ) -> None:
        self.name = name
        self.port = _free_port()
        self.workdir = workdir
        self.admin_password = admin_password
        self._process: subprocess.Popen[bytes] | None = None

    @property
    def url(self) -> str:
        return f"redis://127.0.0.1:{self.port}/0"

    def start(self) -> None:
        assert REDIS_SERVER is not None
        args = [
            REDIS_SERVER,
            "--port",
            str(self.port),
            "--bind",
            "127.0.0.1",
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            str(self.workdir),
        ]

        if self.admin_password is not None:
            args += ["--requirepass", self.admin_password]

        self._process = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        client = redis.Redis(port=self.port, password=self.admin_password)
        deadline = time.monotonic() + 10

        while True:
            try:
                client.ping()
                break
            except redis.ConnectionError:
                if time.monotonic() > deadline:
                    raise

                time.sleep(0.05)

        client.close()

    def stop(self) -> None:
        if self._process is not None:
            self._process.terminate()
            self._process.wait(timeout=10)
            self._process = None

    def client(self) -> Redis:
        """Admin client (default user)."""
        return Redis.from_url(self.url, password=self.admin_password)


@pytest.fixture
def server_factory(tmp_path: Path) -> Iterator[ServerFactory]:
    if REDIS_SERVER is None:
        pytest.skip("redis-server not installed")

    started: list[RedisServer] = []

    def start(admin_password: str | None = None) -> list[RedisServer]:
        nodes = []

        for name in ("a", "b", "c"):
            workdir = tmp_path / name
            workdir.mkdir()
            server = RedisServer(name, workdir, admin_password)
            server.start()
            nodes.append(server)

        started.extend(nodes)
        return nodes

    yield start

    for server in started:
        server.stop()


@pytest.fixture
def servers(server_factory: ServerFactory) -> list[RedisServer]:
    return server_factory()


@pytest.fixture
async def clients(servers: list[RedisServer]) -> AsyncIterator[dict[str, Redis]]:
    connected = {server.name: server.client() for server in servers}
    yield connected

    for client in connected.values():
        await client.aclose()


def write_settings(
    path: Path, servers: list[RedisServer], extra_toml: str = ""
) -> Settings:
    """Write one shared config for *servers* and load it."""
    nodes = "".join(
        f'\n[[nodes]]\nname = "{server.name}"\nurl = "{server.url}"\n'
        for server in servers
    )
    path.write_text(
        '[replication]\npatterns = ["test:*"]\n'
        "stats_interval = 0\nshutdown_drain_timeout = 1.0\n"
        "\n[backoff]\ninitial = 0.05\nmaximum = 0.5\n"
        f"{extra_toml}\n{nodes}",
        encoding="utf-8",
    )
    return load_settings(path)


def for_local(settings: Settings, server: RedisServer) -> Settings:
    """Settings of the process running next to *server*."""
    local = settings.local.model_copy(update={"url": server.url})
    return settings.model_copy(update={"local": local})


async def target_states(replicator: Replicator) -> dict[str, TargetState]:
    return {target.name: target.state for target in replicator.targets}


@asynccontextmanager
async def running_mesh(
    servers: list[RedisServer], settings: Settings
) -> AsyncGenerator[Mesh]:
    """Run one replicator per server, all with the identical configuration."""
    stop = asyncio.Event()
    replicators = {
        server.name: Replicator(for_local(settings, server)) for server in servers
    }
    tasks = [
        asyncio.create_task(replicator.run(stop)) for replicator in replicators.values()
    ]

    try:
        for name, replicator in replicators.items():
            expected = {
                target.name: (
                    TargetState.SELF if target.name == name else TargetState.ACTIVE
                )
                for target in replicator.targets
            }
            await eventually(lambda r=replicator: target_states(r), expected)

        yield replicators
    finally:
        stop.set()
        await asyncio.gather(*tasks)


async def eventually[T](
    probe: Callable[[], Awaitable[T]],
    expected: T,
    timeout: float = 5.0,
) -> None:
    """Poll *probe* until it returns *expected*."""
    deadline = time.monotonic() + timeout
    value = await probe()

    while value != expected:
        if time.monotonic() > deadline:
            msg = f"expected {expected!r}, last value {value!r}"
            raise AssertionError(msg)

        await asyncio.sleep(0.02)
        value = await probe()
