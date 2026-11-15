"""Identity of the local server, used to avoid replicating to ourselves."""

import asyncio

from redis_pmr.config import NodeConfig
from redis_pmr.preflight import ServerInfo, is_same_server


class LocalIdentity:
    """Holds the most recently seen :class:`ServerInfo` of the local server.

    Targets wait for it before their first write and compare run ids to
    detect that a configured node is actually the local server.
    """

    def __init__(self) -> None:
        """Create an identity that is not yet known."""
        self._info: ServerInfo | None = None
        self._known = asyncio.Event()

    @property
    def info(self) -> ServerInfo | None:
        """The local server's info, None until first connected."""
        return self._info

    def update(self, info: ServerInfo) -> None:
        """Record the local server's info after a (re)connect."""
        self._info = info
        self._known.set()

    async def wait(self) -> ServerInfo:
        """Wait until the local server's info is known.

        Returns:
            The most recent info of the local server.

        """
        while self._info is None:
            await self._known.wait()

        return self._info

    def is_local(self, info: ServerInfo) -> bool:
        """Check whether *info* describes the local server.

        Returns:
            True if the run ids match.

        """
        return self._info is not None and is_same_server(self._info, info)


def remote_nodes(nodes: list[NodeConfig], self_name: str | None) -> list[NodeConfig]:
    """Filter out the explicitly named local node.

    Returns:
        All nodes except the one named *self_name*.

    """
    return [node for node in nodes if node.name != self_name]
