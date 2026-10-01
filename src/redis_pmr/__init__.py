"""redis-pmr: poor man's multi-master replication for selected Redis keys."""

from importlib.metadata import version

from redis_pmr.cli import main


__version__ = version("redis-pmr")
__all__ = ["__version__", "main"]
