import requests
import json
import time
from datetime import datetime, timezone
from helpers import db_conn
from helpers import constants


def get_already_collected_apps():
    """gets all of the apps currently in the postgres database"""
    conn = db_conn()
    cursor = conn.cursor()
    query = f"""SELECT id FROM {constants.DATABASE_DUMP_DB}"""
    cursor.execute(query)
    records = [i[0] for i in cursor.fetchall()]
    conn.close()
    return records


def get_app_list():
    """Get app list."""
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response = requests.get(url)
    content = response.content
    content_json = json.loads(content)
    apps = content_json["applist"]["apps"]
    appids = [app["appid"] for app in apps]
    return appids


def filtered_appids():
    """create a list of uncollected ids"""
    in_sql_set = set(get_already_collected_apps())
    appids_set = set(get_app_list())
    return list(appids_set - in_sql_set)


def get_app_data():
    """loop through app ids to retrieve app details and send it to the database"""
    appids = filtered_appids()

    conn = db_conn()
    cursor = conn.cursor()

    for id in appids:
        url = f"https://store.steampowered.com/api/appdetails?appids={id}"
        response = requests.get(url)
        data = json.dumps(response.content.decode("utf-8-sig"))
        print(f"uploading {id}")
        query = cursor.mogrify(
            f"INSERT INTO {constants.DATABASE_DUMP_DB} (id, data, source, timestamp) VALUES (%s, %s)",
            (id, data, "steam_api", datetime.now(timezone.utc).isoformat()),
        )
        cursor.execute(query)
        conn.commit()
        time.sleep(1.5)

    conn.close()


def main():
    get_app_data()


if __name__ == "__main__":
    main()
