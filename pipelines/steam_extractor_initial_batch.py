import requests
import json
import time
from helpers import db_conn
from helpers import constants


def get_app_list():
    """Get app list."""
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response = requests.get(url)
    content = response.content
    content_json = json.loads(content)
    apps = content_json["applist"]["apps"]
    appids = [app["appid"] for app in apps]
    return appids


def get_app_data():
    """loop through app ids to retrieve app details and send it to the database"""
    conn = db_conn()
    cursor = conn.cursor()

    for id in get_app_list():
        url = f"https://store.steampowered.com/api/appdetails?appids={id}"
        response = requests.get(url)
        data = json.dumps(response.json())
        print(f"uploading {id}")
        query = f"""INSERT INTO {constants.DATABASE_DUMP_DB} (id, data)
        VALUES ('{id}',$${data}$$)"""
        cursor.execute(query)
        conn.commit()
        time.sleep(1.5)

    conn.close()
