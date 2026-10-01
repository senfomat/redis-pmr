"""Classification of keyspace notification events."""

from enum import StrEnum


class EventAction(StrEnum):
    """What to do with a keyspace notification."""

    SYNC = "sync"
    IGNORE = "ignore"


IGNORED_EVENTS = frozenset({
    # Written by the replicator itself on the targets; replicating it
    # back would cause ping-pong between the nodes.
    "restore",
    # Every node expires keys on its own (absolute TTLs are replicated).
    "expired",
    # Eviction is a local memory-pressure decision.
    "evicted",
    # Meta events that always accompany a real modification event.
    "new",
    "overwritten",
    "type_changed",
    "keymiss",
})


def classify(event_type: str) -> EventAction:
    """Decide whether a keyspace event marks its key for replication.

    Every event not explicitly ignored marks the key as dirty. The replicator
    then reads the key's current state, so deletions and modifications are
    handled the same way.

    Returns:
        The action for the event.

    """
    return EventAction.IGNORE if event_type in IGNORED_EVENTS else EventAction.SYNC
