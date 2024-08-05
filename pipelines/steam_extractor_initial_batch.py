import pandas as pd
import requests
import json
import time
import psycopg2
import boto3
import sys


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
    for id in get_app_list():
        url = f"https://store.steampowered.com/api/appdetails?appids={id}"
        response = requests.get(url)
        content = response.json()
        for key in content:
            print(key)  # upload id to sql
            print(content[key])  # upload data to data column
            time.sleep(1.5)
