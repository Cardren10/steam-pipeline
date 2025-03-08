from pipelines.steam.steam_extractor import get_app_data
from pipelines.steam.steam_loader import load_data
import helpers
import time
import logging


def main():
    logger = logging.getLogger("steam-pipeline")
    helpers.setup_logger()
    logger.debug("debug")

    start = time.time()

    logging.debug("Getting app data.")
    get_app_data()
    logging.debug("loading data into schema.")
    load_data()

    end = time.time()
    logging.debug(f"pipeline ran for {end - start} seconds")


if __name__ == "__main__":
    main()
