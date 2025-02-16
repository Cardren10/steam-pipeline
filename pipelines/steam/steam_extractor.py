from datetime import datetime, timezone
from helpers import db_conn
import logging
import requests
import json
import time


def get_already_collected_apps():
    """gets all of the apps currently in the postgres database"""
    conn = db_conn()
    cursor = conn.cursor()
    query = "SELECT app_id FROM steam_landing"
    cursor.execute(query)
    records = [i[0] for i in cursor.fetchall()]
    conn.close()
    return records


def get_app_list():
    """Get app list."""
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        raise Exception(e)
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
        try:
            response = requests.get(url)
        except requests.exceptions.RequestException as e:
            raise Exception(e)
        data = json.dumps(response.content.decode("utf-8-sig"))
        logging.debug(f"uploading {id}")
        query = cursor.mogrify(
            "INSERT INTO steam_landing (app_id, app_data, app_source, timestamp, transformed) VALUES (%s, %s, %s, %s, %s)",
            (
                id,
                data,
                "steam_api",
                datetime.now(timezone.utc).isoformat(),
                "0",
            ),
        )
        cursor.execute(query)
        conn.commit()
        time.sleep(1.5)

    conn.close()
