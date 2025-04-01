import psycopg2
from helpers import constants
import json
import pathlib
import logging
import logging.config
import logging.handlers
import atexit
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
import warnings


def db_conn():
    return psycopg2.connect(
        f"dbname={constants.DATABASE_NAME} user={constants.DATABASE_USER} password={constants.DATABASE_PASSWORD} host={constants.DATABASE_ENDPOINT} port={constants.DATABASE_PORT}"
    )


def get_handle_null(d: dict, str: str):
    if d == None:
        return None
    if type(d) == list:
        if len(d) == 0:
            return None
        return d[str]
    if type(d) == dict:
        return d.get(str)


def setup_logger() -> None:
    config = pathlib.Path("logger_config.json")
    with open(config) as file:
        config = json.load(file)
    logging.config.dictConfig(config)
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)


def validate_json(json_str) -> bool:
    try:
        json.loads(json_str)
        return True
    except ValueError:
        return False


def validate_int(int_value):
    if type(int_value) == int:
        return int_value
    return None


def clean_html(text) -> str:
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
    if text == None:
        return None
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text()
