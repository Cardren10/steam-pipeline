from datetime import datetime, timezone
import helpers
import logging
import requests
import json
import time


class SteamExtractor:

    def execute(self):
        """Runs the extractor."""
        logger = logging.getLogger("steam-pipeline")
        helpers.setup_logger()
        logger.debug("debug")

        logging.debug("getting app details.")
        self.get_app_details()
        logging.debug("getting app reviews.")
        self.get_app_reviews()
        logging.debug("getting app tags.")
        self.get_app_tags()

    def get_already_collected_apps(self, source: str) -> list:
        """gets all of the apps currently in the postgres database"""
        conn = helpers.db_conn()
        cursor = conn.cursor()
        query = cursor.mogrify(
            """
            SELECT app_id
            FROM steam_landing
            WHERE source = %s
            """,
            (source),
        )
        cursor.execute(query)
        records = [i[0] for i in cursor.fetchall()]
        conn.close()
        return records

    def get_app_list(self, url: str) -> list:
        """Get app list."""
        try:
            response = requests.get(url)
        except requests.exceptions.RequestException as e:
            raise Exception(e)
        content = response.content
        content_json = json.loads(content)
        apps = content_json["applist"]["apps"]
        appids = [app["appid"] for app in apps]
        return appids

    def filtered_appids(self, source: str, url: str) -> list:
        """create a list of uncollected ids"""
        in_sql_set = set(self.get_already_collected_apps(source))
        appids_set = set(self.get_app_list(url))
        return list(appids_set - in_sql_set)

    def get_app_details(self) -> None:
        """loop through app ids to retrieve app details and send it to the database"""
        appids = self.filtered_appids(
            "steam_api_appdetails",
            "https://api.steampowered.com/ISteamApps/GetAppList/v2/",
        )

        conn = helpers.db_conn()
        cursor = conn.cursor()

        for id in appids:
            url = f"https://store.steampowered.com/api/appdetails?appids={id}"
            try:
                response = requests.get(url)
            except requests.exceptions.RequestException as e:
                raise Exception(e)
            data = json.dumps(response.content.decode("utf-8-sig"))
            if not helpers.validate_json(data):
                logging.warning(f"{id} returned non valid json skipping id.")
                continue
            logging.debug(f"uploading app details for {id}")
            query = cursor.mogrify(
                "INSERT INTO steam_landing (app_id, app_data, app_source, timestamp, transformed) VALUES (%s, %s, %s, %s, %s)",
                (
                    id,
                    data,
                    "steam_api_appdetails",
                    datetime.now(timezone.utc).isoformat(),
                    "0",
                ),
            )
            cursor.execute(query)
            conn.commit()
            time.sleep(1.5)

        conn.close()

    def get_app_reviews(self) -> None:
        """loop through app ids to retrieve app reviews and send it to the database"""

        appids = self.filtered_appids(
            "steam_api_appreviews",
            "https://api.steampowered.com/ISteamApps/GetAppList/v2/",
        )

        conn = helpers.db_conn()
        cursor = conn.cursor()

        for id in appids:
            url = f"https://store.steampowered.com/appreviews/{id}"
            try:
                response = requests.get(url)
            except requests.exceptions.RequestException as e:
                raise Exception(e)
            data = json.dumps(response.content.decode("utf-8-sig"))
            if not helpers.validate_json(data):
                logging.warning(f"{id} returned non valid json skipping id.")
                continue
            logging.debug(f"uploading reviews for {id}")
            query = cursor.mogrify(
                "INSERT INTO steam_landing (app_id, app_data, app_source, timestamp, transformed) VALUES (%s, %s, %s, %s, %s)",
                (
                    id,
                    data,
                    "steam_api_appreviews",
                    datetime.now(timezone.utc).isoformat(),
                    "0",
                ),
            )
            cursor.execute(query)
            conn.commit()
            time.sleep(1.5)

        conn.close()

    def get_app_tags(self):
        """loop through app ids to retrieve app tags and send it to the database"""

        appids = self.filtered_appids(
            "steamspy_tags", "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
        )

        conn = helpers.db_conn()
        cursor = conn.cursor()

        for id in appids:
            url = f"https://steamspy.com/api.php?request=appdetails&appid={id}"
            try:
                response = requests.get(url)
            except requests.exceptions.RequestException as e:
                raise Exception(e)
            data = json.dumps(response.content.decode("utf-8-sig"))
            if not helpers.validate_json(data):
                logging.warning(f"{id} returned non valid json skipping id.")
                continue
            logging.debug(f"uploading tags for {id}")
            query = cursor.mogrify(
                "INSERT INTO steam_landing (app_id, app_data, app_source, timestamp, transformed) VALUES (%s, %s, %s, %s, %s)",
                (
                    id,
                    data,
                    "steamspy_tags",
                    datetime.now(timezone.utc).isoformat(),
                    "0",
                ),
            )
            cursor.execute(query)
            conn.commit()
            time.sleep(1.1)

        conn.close()
