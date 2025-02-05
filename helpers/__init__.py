import psycopg2
from helpers import constants


def db_conn():
    return psycopg2.connect(
        f"dbname={constants.DATABASE_NAME} user={constants.DATABASE_USER} password={constants.DATABASE_PASSWORD} host={constants.DATABASE_ENDPOINT} port={constants.DATABASE_PORT}"
    )


def get_handle_null(dict: dict, str: str):
    if dict == None:
        return None
    if str not in dict:
        return None
    if dict["str"] == None:
        return None
    return dict.get(f"{str}")
