from __future__ import annotations

from asyncio import run


from apscheduler import AsyncScheduler
from apscheduler.triggers.interval import IntervalTrigger

from ccdexplorer_fundamentals.GRPCClient import GRPCClient

from ccdexplorer_fundamentals.GRPCClient.CCD_Types import *  # noqa: F403
from ccdexplorer_fundamentals.tooter import Tooter
from ccdexplorer_fundamentals.mongodb import (
    MongoDB,
    MongoMotor,
)
from SchemaParser import SchemaParser

# from env import *  # noqa: F403
from rich.console import Console

console = Console()

grpcclient = GRPCClient()
tooter = Tooter()
mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)


async def main():
    schema_parser = SchemaParser(grpcclient, tooter, mongodb, motormongo, "testnet")
    async with AsyncScheduler() as scheduler:
        await scheduler.add_schedule(
            schema_parser.get_logged_events,
            IntervalTrigger(seconds=10 * 60),
        )
        await scheduler.run_until_stopped()
        pass


if __name__ == "__main__":
    run(main())
