"""Command line interface."""

import asyncio
import logging
import signal
import sys
from pathlib import Path

import click
from pydantic import ValidationError

from redis_pmr.app import Replicator, run_check
from redis_pmr.config import LogLevel, Settings, load_settings
from redis_pmr.preflight import PreflightError


async def _serve(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await Replicator(settings).run(stop)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="TOML configuration file (identical on all nodes).",
)
@click.option(
    "--node",
    default=None,
    help="Name of the node this process runs on (default: detected by run_id).",
)
@click.option(
    "--log-level",
    type=click.Choice([level.value for level in LogLevel], case_sensitive=False),
    default=LogLevel.INFO.value,
    show_default=True,
)
@click.option("--check", is_flag=True, help="Only check all servers, then exit.")
def main(*, config_path: Path, node: str | None, log_level: str, check: bool) -> None:
    """Replicate selected Redis keys between multiple masters.

    \f

    Raises:
        click.ClickException: On an invalid configuration or a failed preflight.

    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    try:
        settings = load_settings(config_path, node)
    except ValidationError as error:
        message = f"invalid configuration:\n{error}"
        raise click.ClickException(message) from error

    if check:
        results = asyncio.run(run_check(settings))

        for result in results:
            mark = (
                click.style("OK  ", fg="green")
                if result.ok
                else click.style("FAIL", fg="red")
            )
            click.echo(f"{mark} {result.name:<20} {result.message}")

        sys.exit(0 if all(result.ok for result in results) else 1)

    try:
        asyncio.run(_serve(settings))
    except ExceptionGroup as group:
        preflight, rest = group.split(PreflightError)

        if preflight is None or rest is not None:
            raise

        message = str(_first_leaf(preflight))
        raise click.ClickException(message) from group


def _first_leaf(group: BaseExceptionGroup) -> BaseException:
    first = group.exceptions[0]
    return _first_leaf(first) if isinstance(first, BaseExceptionGroup) else first
