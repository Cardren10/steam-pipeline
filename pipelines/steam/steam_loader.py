import helpers
import logging
import json
import time
from psycopg2.extras import execute_values


class SteamLoader:

    def execute(self) -> None:
        logger = logging.getLogger("steam-pipeline")
        helpers.setup_logger()
        logger.debug("debug")

        start = time.time()

        logging.debug("loading app details.")
        self.load_app_details()
        logging.debug("loading app reviews.")
        self.load_app_reviews()
        logging.debug("loading app tags.")
        self.load_app_tags()

        end = time.time()
        logging.debug(f"Loader ran for {end - start} seconds")

    def load_app_details(self) -> None:
        """Loops through all non-transformed app details for appids and loads the data into the schema."""

        conn = helpers.db_conn()
        cursor = conn.cursor()
        query = "SELECT count(*) FROM steam_landing WHERE transformed = '0' AND source = 'steam_api_appdetails'"
        cursor.execute(query)

        rows = cursor.fetchone()[0]
        logging.debug(f"rowcount: {rows}")

        processed = 0
        batch_size = 10
        while processed < rows:
            query = cursor.mogrify(
                """
                    SELECT id, app_id, app_data FROM steam_landing "
                    "WHERE transformed = '0' AND source = 'steam_api_appdetails'
                    LIMIT %s;
                """,
                (batch_size,),
            )
            cursor.execute(query)

            batch = cursor.fetchall()
            landing_ids_to_update = []

            for landing_id, app_id, json_string in batch:
                logging.debug(f"adding details to app: {app_id}")
                if not helpers.validate_json(json_string):
                    logging.warning(f"Invalid JSON found for landing_id: {landing_id}")
                    # Delete the invalid row
                    query = cursor.mogrify(
                        """
                            DELETE FROM steam_landing
                            WHERE id = %s;
                        """,
                        (landing_id,),
                    )
                    cursor.execute(query)
                    continue

                record = json.loads(json_string)
                app_json = record[f"{app_id}"]
                if app_json.get("data") == None:
                    logging.debug(f"app: {app_id} has no data.")

                    # Add empty app to apps.
                    query = cursor.mogrify(
                        """
                            INSERT INTO apps (app_id)
                            VALUES (%s)
                        """,
                        (app_id,),
                    )
                    cursor.execute(query)

                    # add the landing id to update list
                    landing_ids_to_update.append(landing_id)
                    continue

                landing_ids_to_update.append(landing_id)
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
                        app_id,
                        helpers.get_handle_null(data, "name"),
                        helpers.get_handle_null(data, "type"),
                        helpers.validate_int(
                            helpers.get_handle_null(data, "required_age")
                        ),
                        helpers.get_handle_null(data, "is_free"),
                        helpers.get_handle_null(data, "controller_support"),
                        helpers.clean_html(
                            helpers.get_handle_null(data, "detailed_description")
                        ),
                        helpers.clean_html(
                            helpers.get_handle_null(data, "about_the_game")
                        ),
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
                        helpers.clean_html(
                            helpers.get_handle_null(pc_req, "recommended")
                        ),
                        helpers.clean_html(helpers.get_handle_null(mac_req, "minimum")),
                        helpers.clean_html(
                            helpers.get_handle_null(mac_req, "recommended")
                        ),
                        helpers.clean_html(helpers.get_handle_null(lin_req, "minimum")),
                        helpers.clean_html(
                            helpers.get_handle_null(lin_req, "recommended")
                        ),
                        helpers.clean_html(
                            helpers.get_handle_null(data, "legal_notice")
                        ),
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

                # Query to update genres and app_genres table if necessary
                genres = helpers.get_handle_null(data, "genres")
                if genres != None:
                    genre_values = []
                    app_genres = []

                    for genre in genres:
                        genre_values.append(
                            (
                                helpers.get_handle_null(genre, "id"),
                                helpers.get_handle_null(genre, "description"),
                            )
                        )
                        app_genres.append(
                            (
                                app_id,
                                helpers.get_handle_null(genre, "id"),
                            )
                        )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO genres (genre_id, genre)
                            VALUES %s
                            ON CONFLICT (genre_id) DO NOTHING;
                        """,
                        genre_values,
                    )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO app_genres (app_id, genre_id)
                            VALUES %s
                        """,
                        app_genres,
                    )

                # Query that inserts into the dlc table if necissary.
                dlc = helpers.get_handle_null(data, "dlc")
                if dlc != None and len(dlc) > 0:
                    dlc_values = []
                    for dlc_id in dlc:
                        dlc_values.append(
                            (
                                app_id,
                                dlc_id,
                            )
                        )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO dlcs (app_id, dlc_id)
                            VALUES %s
                        """,
                        dlc_values,
                    )

                # Query that inserts developers table.
                developers = helpers.get_handle_null(data, "developers")
                if developers != None and len(developers) > 0:
                    dev_values = []
                    for dev in developers:
                        dev_values.append(
                            (
                                app_id,
                                dev,
                            )
                        )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO developers (app_id, developer)
                            VALUES %s
                        """,
                        dev_values,
                    )

                # Query that inserts into the publishers table.
                publishers = helpers.get_handle_null(data, "publishers")
                if publishers != None and len(publishers) > 0:
                    publisher_values = []
                    for publisher in publishers:
                        publisher_values.append(
                            (
                                app_id,
                                publisher,
                            )
                        )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO publishers (app_id, publisher)
                            VALUES %s
                        """,
                        publisher_values,
                    )

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
                            app_id,
                            helpers.get_handle_null(price, "initial"),
                            helpers.get_handle_null(price, "final"),
                        ),
                    )

                    cursor.execute(query)

                # Query that inserts into the packages table.
                packages = helpers.get_handle_null(data, "packages")
                if packages != None:
                    package_values = []
                    for package in packages:
                        package_values.append(
                            (
                                app_id,
                                package,
                            )
                        )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO packages (app_id, package_id)
                            VALUES %s;
                        """,
                        package_values,
                    )

                # Query that inserts in the categories and app_categories table.
                categories = helpers.get_handle_null(data, "categories")
                if categories != None:
                    category_values = []
                    app_category_values = []
                    for category in categories:
                        category_values.append(
                            (
                                helpers.get_handle_null(category, "id"),
                                helpers.get_handle_null(category, "description"),
                            )
                        )
                        app_category_values.append(
                            (
                                app_id,
                                helpers.get_handle_null(category, "id"),
                            )
                        )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO categories (category_id, category)
                            VALUES %s
                            ON CONFLICT (category_id) DO NOTHING; 
                        """,
                        category_values,
                    )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO app_categories (app_id, category_id)
                            VALUES %s
                        """,
                        app_category_values,
                    )

                # Query that inserts into the app_screenshots table
                screenshots = helpers.get_handle_null(data, "screenshots")
                if screenshots != None and len(screenshots) > 0:
                    screenshot_values = []
                    for screenshot in screenshots:
                        screenshot_values.append(
                            (
                                app_id,
                                helpers.get_handle_null(screenshot, "id"),
                                helpers.get_handle_null(screenshot, "path_thumbnail"),
                                helpers.get_handle_null(screenshot, "path_full"),
                            )
                        )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO app_screenshots (app_id, app_screenshots_id, path_thumbnail, path_full)
                            VALUES %s
                        """,
                        screenshot_values,
                    )

                # Query that inserts into the movies table.
                movies = helpers.get_handle_null(data, "movies")
                if movies != None and len(movies) > 0:
                    movie_values = []
                    for movie in movies:
                        webm = helpers.get_handle_null(movie, "webm")
                        mp4 = helpers.get_handle_null(movie, "mp4")
                        movie_values.append(
                            (
                                app_id,
                                helpers.get_handle_null(movie, "id"),
                                helpers.get_handle_null(movie, "name"),
                                helpers.get_handle_null(movie, "thumbnail"),
                                helpers.get_handle_null(webm, "480"),
                                helpers.get_handle_null(webm, "max"),
                                helpers.get_handle_null(mp4, "480"),
                                helpers.get_handle_null(mp4, "max"),
                                helpers.get_handle_null(movie, "highlight"),
                            )
                        )

                    execute_values(
                        cursor,
                        """
                            INSERT INTO movies(app_id, movie_id, movie_name, thumbnail, webm_480, webm_max, mp4_480, mp4_max, highlight)
                            VALUES %s
                            ON CONFLICT (movie_id) DO NOTHING;
                        """,
                        movie_values,
                    )

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
                        app_id,
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

            # Update steam landing to reflect transformation
            if landing_ids_to_update:
                cursor.execute(
                    "UPDATE steam_landing SET transformed = '1' WHERE id = ANY(%s)",
                    (landing_ids_to_update,),
                )

            conn.commit()

            processed += len(batch)
            logging.info(f"Processed {processed}/{rows} app details")
        conn.close()

    def load_app_reviews(self) -> None:

        conn = helpers.db_conn()
        cursor = conn.cursor()
        query = "SELECT count(*) FROM steam_landing WHERE transformed = '0' AND source = 'steam_api_appreviews'"
        cursor.execute(query)

        rows = cursor.fetchone()[0]
        logging.debug(f"rowcount: {rows}")

        processed = 0
        batch_size = 10
        while processed < rows:
            cursor.execute(
                "SELECT id, app_id, app_data FROM steam_landing "
                "WHERE transformed = '0' AND source = 'steam_api_appreviews' "
                f"LIMIT {batch_size}"
            )

            batch = cursor.fetchall()
            landing_ids_to_update = []

            for landing_id, app_id, json_string in batch:

                if not helpers.validate_json(json_string):
                    logging.warning(f"Invalid json found for landing_id: {landing_id}")
                    # Delete the invalid row
                    query = cursor.mogrify(
                        """
                            DELETE FROM steam_landing
                            WHERE id = %s;
                        """,
                        (landing_id,),
                    )
                    cursor.execute(query)
                    continue

                app_json = json.loads(json_string)
                if app_json.get("query_summary") == None:
                    logging.debug(f"app: {app_id} has no data.")
                    landing_ids_to_update.append(landing_id)
                    continue

                # add landing id to update list
                landing_ids_to_update.append(landing_id)

                data = app_json["query_summary"]

                # upsert in case of no apps.
                app_query = cursor.mogrify(
                    """
                        INSERT INTO apps (app_id)
                        VALUES (%s)
                        ON CONFLICT (app_id) DO NOTHING;
                    """,
                    (app_id,),
                )
                cursor.execute(app_query)

                # Query that inserts into the reviews table.
                query = cursor.mogrify(
                    """
                        INSERT INTO reviews 
                        (
                            app_id,
                            review_score,
                            review_score_desc,
                            total_positive,
                            total_negative,
                            total_reviews
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (
                        app_id,
                        helpers.get_handle_null(data, "review_score"),
                        helpers.get_handle_null(data, "review_score_desc"),
                        helpers.get_handle_null(data, "total_positive"),
                        helpers.get_handle_null(data, "total_negative"),
                        helpers.get_handle_null(data, "total_reviews"),
                    ),
                )
                cursor.execute(query)

            # Update steam landing to reflect transformation
            if landing_ids_to_update:
                cursor.execute(
                    "UPDATE steam_landing SET transformed = '1' WHERE id = ANY(%s)",
                    (landing_ids_to_update,),
                )

            processed += len(batch)
            logging.info(f"Processed {processed}/{rows} app reviews")

            # When Psycopg2 executes a query it does so as a transaction that does not complete until commit is run. Any failure before this should send an error that includes what appid specifically failed.
            conn.commit()

        conn.close()

    def load_app_tags(self) -> None:
        conn = helpers.db_conn()
        cursor = conn.cursor()
        query = "SELECT count(*) FROM steam_landing WHERE transformed = '0' AND source = 'steamspy_tags'"
        cursor.execute(query)

        rows = cursor.fetchone()[0]
        logging.debug(f"rowcount: {rows}")

        processed = 0
        batch_size = 10
        while processed < rows:

            cursor.execute(
                "SELECT id, app_id, app_data FROM steam_landing "
                "WHERE transformed = '0' AND source = 'steamspy_tags' "
                f"LIMIT {batch_size}"
            )

            batch = cursor.fetchall()
            landing_ids_to_update = []

            for landing_id, app_id, json_string in batch:
                logging.debug(f"adding tags to app: {app_id}")
                app_json = json.loads(json_string)

                if not helpers.validate_json(json_string):
                    logging.warning(f"Invalid json found for landing_id: {landing_id}")
                    # Delete the invalid row
                    query = cursor.mogrify(
                        """
                            DELETE FROM steam_landing
                            WHERE id = %s;
                        """,
                        (landing_id,),
                    )
                    cursor.execute(query)
                    continue

                if app_json.get("tags") == None:
                    logging.debug(f"app: {app_id} has no data.")
                    landing_ids_to_update.append(landing_id)
                    continue

                tags = app_json["tags"]

                if len(tags) == 0:
                    logging.debug(f"app: {app_id} has no tags.")
                    landing_ids_to_update.append(landing_id)
                    continue

                # add the id to update list.
                landing_ids_to_update.append(landing_id)

                # upsert in case of missing app
                app_query = cursor.mogrify(
                    """
                    INSERT INTO apps (app_id)
                    VALUES (%s)
                    ON CONFLICT (app_id) DO NOTHING;
                    """,
                    (app_id,),
                )
                cursor.execute(app_query)

                # Query that inserts into the tags table.
                tag_values = []
                app_tag_values = []

                for k, v in tags.items():
                    tag_values.append(
                        (
                            v,
                            k,
                        )
                    )
                    app_tag_values.append(
                        (
                            app_id,
                            v,
                        )
                    )

                execute_values(
                    cursor,
                    """
                        INSERT INTO tags (tag_id, tag)
                        VALUES %s
                        ON CONFLICT (tag_id) DO NOTHING;
                    """,
                    tag_values,
                )

                execute_values(
                    cursor,
                    """
                        INSERT INTO app_tags (app_id, tag_id)
                        VALUES %s;
                    """,
                    app_tag_values,
                )

            # Update steam landing to reflect transformation
            if landing_ids_to_update:
                cursor.execute(
                    "UPDATE steam_landing SET transformed = '1' WHERE id = ANY(%s)",
                    (landing_ids_to_update,),
                )

            processed += len(batch)
            logging.info(f"Processed {processed}/{rows} app tags")

            # When Psycopg2 executes a query it does so as a transaction that does not complete until commit is run. Any failure before this should send an error that includes what appid specifically failed.
            conn.commit()

        conn.close()
