from pipelines.steam.steam_extractor import SteamExtractor
from pipelines.steam.steam_loader import SteamLoader
import helpers
import time
import logging


def main():
    logger = logging.getLogger("steam-pipeline")
    helpers.setup_logger()
    logger.debug("debug")

    start = time.time()

    logging.debug("Getting app data.")
    SteamExtractor.execute()
    logging.debug("loading data into schema.")
    SteamLoader.execute()

    end = time.time()
    logging.debug(f"pipeline ran for {end - start} seconds")


if __name__ == "__main__":
    main()
