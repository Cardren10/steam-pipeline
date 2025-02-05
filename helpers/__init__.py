import psycopg2
from helpers import constants


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
