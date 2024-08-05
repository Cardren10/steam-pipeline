import psycopg2
from helpers import constants


def db_conn():
    return psycopg2.connect(
        f"dbname={constants.DATABASE_NAME} user={constants.DATABASE_USER} password={constants.DATABASE_PASSWORD} host={constants.DATABASE_ENDPOINT} port={constants.DATABASE_PORT}"
    )
