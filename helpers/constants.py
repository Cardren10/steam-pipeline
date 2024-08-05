import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), "../config.conf"))


# DataBase
DATABASE_ENDPOINT = parser.get("database", "database_endpoint")
DATABASE_HOST = parser.get("database", "database_host")
DATABASE_NAME = parser.get("database", "database_name")
DATABASE_PORT = parser.get("database", "database_port")
DATABASE_USER = parser.get("database", "database_username")
DATABASE_PASSWORD = parser.get("database", "database_password")
DATABASE_DUMP_DB = parser.get("database", "database_dump_db")
