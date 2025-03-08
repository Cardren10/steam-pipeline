import helpers
import logging
import json


class SteamLoader:

    def execute(self) -> None:
        logger = logging.getLogger("steam-pipeline")
        helpers.setup_logger()
        logger.debug("debug")

        logging.debug("loading app details.")
        self.load_app_details()

    def load_app_details() -> None:
        """Loops through all non-transformed app details for appids and loads the data into the schema."""

        conn = helpers.db_conn()
        cursor = conn.cursor()
        query = "SELECT count(*) FROM steam_landing WHERE transformed = '0' AND source = 'steam_api_appdetails'"
        cursor.execute(query)
        rows = cursor.fetchone()[0]
        logging.debug(f"rowcount: {rows}")

        for row in range(rows):
            query = "SELECT id, app_data FROM steam_landing WHERE transformed = '0' AND source = 'steam_api_appdetails' LIMIT 1"
            cursor.execute(query)
            json_string = cursor.fetchone()[1]
            if not helpers.validate_json(json_string):

                query = "SELECT id, app_data FROM steam_landing WHERE transformed = '0' AND source = 'steam_api_appdetails' LIMIT 1"
                cursor.execute(query)
                landing_id = cursor.fetchone()[0]
                logging.warning(f"Invalid json found for landing_id: {landing_id}")

                # Update steam_landing to transformed
                query = cursor.mogrify(
                    """
                    UPDATE steam_landing
                    SET transformed = %s
                    WHERE id = %s
                    """,
                    ("1", landing_id),
                )
                cursor.execute(query)

                conn.commit()
                continue

            record = json.loads(json_string)
            appid = next(iter(record))
            logging.debug(f"appid: {appid}")
            app_json = record[f"{appid}"]
            logging.debug(f"app: {app_json}")

            if app_json.get("data") == None:
                logging.debug(f"app: {appid} has no data.")

                # Add empty app to apps.
                query = cursor.mogrify(
                    """
                        INSERT INTO apps (app_id)
                        VALUES (%s)
                    """,
                    (appid,),
                )
                cursor.execute(query)

                # Update steam_landing to transformed
                query = cursor.mogrify(
                    """
                    UPDATE steam_landing
                    SET transformed = %s
                    WHERE app_id = %s AND source = %s
                    """,
                    ("1", appid, "steam_api_appdetails"),
                )
                cursor.execute(query)

                conn.commit()
                continue

            data = app_json["data"]

            # Query that inserts into the apps table.
            pc_req = helpers.get_handle_null(data, "pc_requirements")
            mac_req = helpers.get_handle_null(data, "mac_requirements")
            lin_req = helpers.get_handle_null(data, "linux_requirements")
            platforms = helpers.get_handle_null(data, "platforms")
            achievements = helpers.get_handle_null(data, "achievements")
            release_date = helpers.get_handle_null(data, "release_date")
            support_info = helpers.get_handle_null(data, "support_info")

            query = cursor.mogrify(
                """
                    INSERT INTO apps 
                    (
                        app_id,
                        app_name,
                        app_type,
                        required_age,
                        is_free,
                        controller_support,
                        detailed_description,
                        about_the_game,
                        short_description,
                        supported_languages,
                        reviews,
                        header_image,
                        capsule_image,
                        capsule_imagev5,
                        website,
                        pc_requirements_minimum,
                        pc_requirements_recommended,
                        mac_requirements_minimum,
                        mac_requirements_recommended,
                        linux_requirements_minimum,
                        linux_requirements_recommended,
                        legal_notice,
                        platform_windows,
                        platform_mac,
                        platform_linux,
                        total_achievements,
                        coming_soon,
                        release_date,
                        support_url,
                        support_email,
                        background,
                        background_raw
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                (
                    appid,
                    helpers.get_handle_null(data, "name"),
                    helpers.get_handle_null(data, "type"),
                    helpers.validate_int(helpers.get_handle_null(data, "required_age")),
                    helpers.get_handle_null(data, "is_free"),
                    helpers.get_handle_null(data, "controller_support"),
                    helpers.clean_html(
                        helpers.get_handle_null(data, "detailed_description")
                    ),
                    helpers.clean_html(helpers.get_handle_null(data, "about_the_game")),
                    helpers.clean_html(
                        helpers.get_handle_null(data, "short_description")
                    ),
                    helpers.clean_html(
                        helpers.get_handle_null(data, "supported_languages")
                    ),
                    helpers.clean_html(helpers.get_handle_null(data, "reviews")),
                    helpers.get_handle_null(data, "header_image"),
                    helpers.get_handle_null(data, "capsule_image"),
                    helpers.get_handle_null(data, "capsule_imagev5"),
                    helpers.get_handle_null(data, "website"),
                    helpers.clean_html(helpers.get_handle_null(pc_req, "minimum")),
                    helpers.clean_html(helpers.get_handle_null(pc_req, "recommended")),
                    helpers.clean_html(helpers.get_handle_null(mac_req, "minimum")),
                    helpers.clean_html(helpers.get_handle_null(mac_req, "recommended")),
                    helpers.clean_html(helpers.get_handle_null(lin_req, "minimum")),
                    helpers.clean_html(helpers.get_handle_null(lin_req, "recommended")),
                    helpers.clean_html(helpers.get_handle_null(data, "legal_notice")),
                    helpers.get_handle_null(platforms, "windows"),
                    helpers.get_handle_null(platforms, "mac"),
                    helpers.get_handle_null(platforms, "linux"),
                    helpers.validate_int(
                        helpers.get_handle_null(achievements, "total")
                    ),
                    helpers.get_handle_null(release_date, "coming_soon"),
                    helpers.get_handle_null(release_date, "date"),
                    helpers.get_handle_null(support_info, "url"),
                    helpers.get_handle_null(support_info, "email"),
                    helpers.get_handle_null(data, "background"),
                    helpers.get_handle_null(data, "background_raw"),
                ),
            )
            cursor.execute(query)

            # Query to update genres table if necessary
            genres = helpers.get_handle_null(data, "genres")
            if genres != None:
                for genre in genres:
                    query = cursor.mogrify(
                        """
                            INSERT INTO genres 
                            (
                                genre_id,
                                genre
                            )
                            SELECT %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM genres
                                WHERE genres.genre_id = %s
                            );""",
                        (
                            helpers.get_handle_null(genre, "id"),
                            helpers.get_handle_null(genre, "description"),
                            helpers.get_handle_null(genre, "id"),
                        ),
                    )
                    cursor.execute(query)

            # Query that inserts in the app_genres tables.
            if genres != None:
                logging.debug(f"genres found for app:{appid}")
                for genre in genres:
                    query = cursor.mogrify(
                        """
                            INSERT INTO app_genres 
                            (
                                app_id,
                                genre_id
                            ) VALUES (%s, %s);""",
                        (appid, helpers.get_handle_null(genre, "id")),
                    )
                    cursor.execute(query)

            # Query that inserts into the dlc table if necissary.
            dlc = helpers.get_handle_null(data, "dlc")
            if dlc != None:
                logging.debug(f"dlc found for app:{appid}")
                if len(dlc) > 0:
                    for dlc_id in dlc:
                        query = cursor.mogrify(
                            """
                                INSERT INTO dlcs
                                (
                                    app_id,
                                    dlc_id
                                )
                                VALUES (%s, %s);
                            """,
                            (appid, dlc_id),
                        )
                        cursor.execute(query)

            # Query that inserts developers table.
            developers = helpers.get_handle_null(data, "developers")
            if developers != None:
                logging.debug(f"developers found for app:{appid}")
                if len(developers) > 0:
                    for dev in developers:
                        query = cursor.mogrify(
                            """
                                INSERT INTO developers
                                (
                                    app_id,
                                    developer
                                )
                                VALUES (%s, %s);
                            """,
                            (appid, dev),
                        )
                        cursor.execute(query)

            # Query that inserts into the publishers table.
            publishers = helpers.get_handle_null(data, "publishers")
            if publishers != None:
                if len(publishers) > 0:
                    for publisher in publishers:
                        query = cursor.mogrify(
                            """
                                INSERT INTO publishers
                                (
                                    app_id,
                                    publisher
                                )
                                VALUES (%s, %s);
                            """,
                            (appid, publisher),
                        )
                        cursor.execute(query)

            # Query that inserts in the prices table
            price = helpers.get_handle_null(data, "price_overview")
            if price != None:
                query = cursor.mogrify(
                    """
                        INSERT INTO prices
                        (
                            app_id,
                            initial,
                            final_price
                        )
                        VALUES (%s, %s, %s);
                    """,
                    (
                        appid,
                        helpers.get_handle_null(price, "initial"),
                        helpers.get_handle_null(price, "final"),
                    ),
                )
                cursor.execute(query)

            # Query that inserts into the packages table.
            packages = helpers.get_handle_null(data, "packages")
            if packages != None:
                for package in packages:
                    query = cursor.mogrify(
                        """
                            INSERT INTO packages
                            (
                                app_id,
                                package_id
                            )
                            VALUES (%s, %s);
                        """,
                        (appid, package),
                    )
                    cursor.execute(query)

            # Query that inserts in the categories table.
            categories = helpers.get_handle_null(data, "categories")
            if categories != None:
                for category in categories:
                    query = cursor.mogrify(
                        """
                            INSERT INTO categories 
                            (
                                category_id,
                                category
                            )
                            SELECT %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM categories
                                WHERE categories.category_id = %s
                            );
                        """,
                        (
                            helpers.get_handle_null(category, "id"),
                            helpers.get_handle_null(category, "description"),
                            helpers.get_handle_null(category, "id"),
                        ),
                    )
                    cursor.execute(query)

            # Query that inserts in the app_categories tables.
            if categories != None:
                for category in categories:
                    query = cursor.mogrify(
                        """
                            INSERT INTO app_categories 
                            (
                                app_id,
                                category_id
                            ) VALUES (%s, %s);
                        """,
                        (appid, helpers.get_handle_null(category, "id")),
                    )
                    cursor.execute(query)

            # Query that inserts into the app_screenshots table
            screenshots = helpers.get_handle_null(data, "screenshots")
            if screenshots != None:
                for screenshot in screenshots:
                    query = cursor.mogrify(
                        """
                            INSERT INTO app_screenshots 
                            (
                                app_id,
                                app_screenshots_id,
                                path_thumbnail,
                                path_full
                            ) VALUES (%s, %s, %s, %s)
                        """,
                        (
                            appid,
                            helpers.get_handle_null(screenshot, "id"),
                            helpers.get_handle_null(screenshot, "path_thumbnail"),
                            helpers.get_handle_null(screenshot, "path_full"),
                        ),
                    )
                    cursor.execute(query)

            # Query that inserts into the movies table.
            movies = helpers.get_handle_null(data, "movies")
            if movies != None:
                for movie in movies:
                    webm = helpers.get_handle_null(movie, "webm")
                    mp4 = helpers.get_handle_null(movie, "mp4")
                    query = cursor.mogrify(
                        """
                            INSERT INTO movies 
                            (
                                app_id,
                                movie_id,
                                movie_name,
                                thumbnail,
                                webm_480,
                                webm_max,
                                mp4_480,
                                mp4_max,
                                highlight
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (movie_id) DO NOTHING;
                        """,
                        (
                            appid,
                            helpers.get_handle_null(movie, "id"),
                            helpers.get_handle_null(movie, "name"),
                            helpers.get_handle_null(movie, "thumbnail"),
                            helpers.get_handle_null(webm, "480"),
                            helpers.get_handle_null(webm, "max"),
                            helpers.get_handle_null(mp4, "480"),
                            helpers.get_handle_null(mp4, "max"),
                            helpers.get_handle_null(movie, "highlight"),
                        ),
                    )
                    cursor.execute(query)

            # Query to add ratings to the ratings table.
            ratings = helpers.get_handle_null(data, "ratings")
            esrb = helpers.get_handle_null(ratings, "esrb")
            oflc = helpers.get_handle_null(ratings, "oflc")
            crl = helpers.get_handle_null(ratings, "crl")
            usk = helpers.get_handle_null(ratings, "usk")
            dejus = helpers.get_handle_null(ratings, "dejus")
            cero = helpers.get_handle_null(ratings, "cero")
            kgrb = helpers.get_handle_null(ratings, "kgrb")
            csrr = helpers.get_handle_null(ratings, "cssr")
            steam_germany = helpers.get_handle_null(ratings, "steam_germany")
            query = cursor.mogrify(
                """
                    INSERT INTO ratings 
                    (
                        app_id,
                        esrb_rating,
                        esrb_descriptors,
                        esrb_age_gate,
                        esrb_required_age,
                        oflc_rating,
                        oflc_descriptors,
                        oflc_age_gate,
                        oflc_required_age,
                        crl_rating,
                        crl_descriptors,
                        crl_age_gate,
                        crl_required_age,
                        usk_rating,
                        usk_descriptors,
                        usk_age_gate,
                        usk_required_age,
                        usk_rating_id,
                        dejus_rating,
                        dejus_descriptors,
                        dejus_age_gate,
                        dejus_required_age,
                        cero_rating,
                        cero_descriptors,
                        kgrb_rating,
                        kgrb_descriptors,
                        csrr_rating,
                        csrr_descriptors,
                        steam_germany_rating,
                        steam_germany_descriptors,
                        steam_germany_age_gate,
                        steam_germany_required_age,
                        steam_germany_banned
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    appid,
                    helpers.get_handle_null(esrb, "rating"),
                    helpers.get_handle_null(esrb, "descriptors"),
                    helpers.get_handle_null(esrb, "use_age_gate"),
                    helpers.get_handle_null(esrb, "required_age"),
                    helpers.get_handle_null(oflc, "rating"),
                    helpers.get_handle_null(oflc, "descriptors"),
                    helpers.get_handle_null(oflc, "use_age_gate"),
                    helpers.get_handle_null(oflc, "required_age"),
                    helpers.get_handle_null(crl, "rating"),
                    helpers.get_handle_null(crl, "descriptors"),
                    helpers.get_handle_null(crl, "use_age_gate"),
                    helpers.get_handle_null(crl, "required_age"),
                    helpers.get_handle_null(usk, "rating"),
                    helpers.get_handle_null(usk, "descriptors"),
                    helpers.get_handle_null(usk, "use_age_gate"),
                    helpers.get_handle_null(usk, "required_age"),
                    helpers.get_handle_null(usk, "rating_id"),
                    helpers.get_handle_null(dejus, "rating"),
                    helpers.get_handle_null(dejus, "descriptors"),
                    helpers.get_handle_null(dejus, "use_age_gate"),
                    helpers.get_handle_null(dejus, "required_age"),
                    helpers.get_handle_null(cero, "rating"),
                    helpers.get_handle_null(cero, "descriptors"),
                    helpers.get_handle_null(kgrb, "rating"),
                    helpers.get_handle_null(kgrb, "descriptors"),
                    helpers.get_handle_null(csrr, "rating"),
                    helpers.get_handle_null(csrr, "descriptors"),
                    helpers.get_handle_null(steam_germany, "rating"),
                    helpers.get_handle_null(steam_germany, "descriptors"),
                    helpers.get_handle_null(steam_germany, "use_age_gate"),
                    helpers.get_handle_null(steam_germany, "required_age"),
                    helpers.get_handle_null(steam_germany, "banned"),
                ),
            )
            cursor.execute(query)

            # Query to update the landing transformed value.
            query = cursor.mogrify(
                """
                    UPDATE steam_landing
                    SET transformed = %s
                    WHERE app_id = %s AND source = %s
                """,
                ("1", appid, "steam_api_appdetails"),
            )
            cursor.execute(query)

            # When Psycopg2 executes a query it does so as a transaction that does not complete until commit is run. Any failure before this should send an error that includes what appid specifically failed.
            conn.commit()

        conn.close()
