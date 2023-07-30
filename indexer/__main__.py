import asyncio
import traceback
from listener import Listener
from apibara.indexer import IndexerRunner, IndexerRunnerConfiguration
from pymongo import MongoClient
from config import TomlConfig
import json
from logger import Logger


async def main():
    conf = TomlConfig("config.toml", "config.template.toml")
    logger = Logger(conf)
    events_manager = Listener(conf, logger)
    runner = IndexerRunner(
        config=IndexerRunnerConfiguration(
            stream_url=conf.apibara_stream,
            storage_url=conf.connection_string,
            token=conf.token,
            stream_ssl=True,
        ),
        reset_state=conf.reset_state,
    )
    logger.info("starting sales indexer")
    await runner.run(events_manager, ctx={"network": "starknet-mainnet"})


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            conf = TomlConfig(
                "config.toml", "config.template.toml"
            )  # create a new config object
            logger = Logger(conf)  # create an instance of Logger
            exception_traceback = traceback.format_exc()  # get the traceback
            print(exception_traceback)  # print it locally
            logger.warning(
                f"warning: {type(e).__name__} detected, restarting"
            )  # only send the exception type to the server
