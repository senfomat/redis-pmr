"""Bounded, coalescing work buffer."""

import asyncio
from collections.abc import Callable, Iterable


class CoalescingBuffer[K, V]:
    """Insertion-ordered key/value buffer where newer values replace older ones.

    Putting a key that is already pending replaces (or merges) its value and
    moves it to the end, so a burst of changes to one key results in a single
    unit of work.
    """

    def __init__(
        self,
        max_size: int | None = None,
        merge: Callable[[V, V], V] | None = None,
    ) -> None:
        """Create a buffer.

        Args:
            max_size: Maximum number of distinct pending keys, None for unbounded.
            merge: Combines the pending (old) and the new value. Without it
                the new value wins.

        """
        self._items: dict[K, V] = {}
        self._max_size = max_size
        self._merge = merge
        self._not_empty = asyncio.Event()

    def __len__(self) -> int:
        """Count the pending keys.

        Returns:
            The number of pending keys.

        """
        return len(self._items)

    def put(self, key: K, value: V) -> bool:
        """Add or replace *key*.

        Returns:
            False if the key was new and the buffer is full (the item is
            dropped), True otherwise.

        """
        if (old := self._items.pop(key, _MISSING)) is not _MISSING:
            if self._merge is not None:
                value = self._merge(old, value)  # type: ignore[arg-type]

        elif self._max_size is not None and len(self._items) >= self._max_size:
            return False

        self._items[key] = value
        self._not_empty.set()
        return True

    def requeue(self, items: Iterable[tuple[K, V]]) -> None:
        """Put failed items back in front of the buffer.

        Items whose key was put again in the meantime are merged with (or
        superseded by) the newer pending value, which keeps its position.
        """
        front: dict[K, V] = {}

        for key, failed in items:
            value = failed

            if (newer := self._items.pop(key, _MISSING)) is not _MISSING:
                value = (
                    self._merge(failed, newer)  # type: ignore[arg-type]
                    if self._merge is not None
                    else newer
                )

            front[key] = value

        if front:
            front.update(self._items)
            self._items = front
            self._not_empty.set()

    def take(self, max_items: int) -> list[tuple[K, V]]:
        """Remove up to *max_items* of the oldest items.

        Returns:
            The removed items, oldest first.

        """
        batch: list[tuple[K, V]] = []

        for key in self._items:
            if len(batch) >= max_items:
                break

            batch.append((key, self._items[key]))

        for key, _ in batch:
            del self._items[key]

        if not self._items:
            self._not_empty.clear()

        return batch

    async def drain(self, max_items: int) -> list[tuple[K, V]]:
        """Wait until items are pending, then take up to *max_items* of them.

        Returns:
            The removed items, oldest first (never empty).

        """
        while not self._items:
            await self._not_empty.wait()

        return self.take(max_items)

    def clear(self) -> None:
        """Drop all pending items."""
        self._items.clear()
        self._not_empty.clear()


_MISSING = object()
