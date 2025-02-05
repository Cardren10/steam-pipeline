from helpers import db_conn, get_handle_null
from helpers import constants
import json


def load_data() -> None:
    conn = db_conn()
    cursor = conn.cursor()
    query = "SELECT id, app_data FROM steam_landing WHERE transformed = '0'"
    cursor.execute(query)
    rows = cursor.rowcount
    for i in range(rows):
        query = "SELECT id, app_data FROM steam_landing WHERE transformed = '0'"
        cursor.execute(query)
        json_string = cursor.fetchone()[1]
        record = json.loads(json_string)
        appid = next(iter(record))
        print(f"appid: {appid}")
        app_json = record[f"{appid}"]
        print(f"app: {app_json}")

        if app_json.get("data") == None:
            print(f"app: {appid} has no data.")

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
                WHERE app_id = %s
                """,
                ("1", appid),
            )
            cursor.execute(query)

            conn.commit()
            continue

        data = app_json["data"]

        # Query that inserts into the apps table.
        pc_req = get_handle_null(data, "pc_requirements")
        mac_req = get_handle_null(data, "mac_requirements")
        print(f"mac_reqs: {mac_req}")
        print(f"mac_req type: {type(mac_req)}")
        lin_req = get_handle_null(data, "linux_requirements")
        platforms = get_handle_null(data, "platforms")
        achievements = get_handle_null(data, "achievements")
        release_date = get_handle_null(data, "release_date")
        support_info = get_handle_null(data, "support_info")

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
                get_handle_null(data, "name"),
                get_handle_null(data, "type"),
                get_handle_null(data, "required_age"),
                get_handle_null(data, "is_free"),
                get_handle_null(data, "controller_support"),
                get_handle_null(data, "detailed_description"),
                get_handle_null(data, "about_the_game"),
                get_handle_null(data, "short_description"),
                get_handle_null(data, "supported_languages"),
                get_handle_null(data, "reviews"),
                get_handle_null(data, "header_image"),
                get_handle_null(data, "capsule_image"),
                get_handle_null(data, "capsule_imagev5"),
                get_handle_null(data, "website"),
                get_handle_null(pc_req, "minimum"),
                get_handle_null(pc_req, "recommended"),
                get_handle_null(mac_req, "minimum"),
                get_handle_null(mac_req, "recommended"),
                get_handle_null(lin_req, "minimum"),
                get_handle_null(lin_req, "recommended"),
                get_handle_null(data, "legal_notice"),
                get_handle_null(platforms, "windows"),
                get_handle_null(platforms, "mac"),
                get_handle_null(platforms, "linux"),
                get_handle_null(achievements, "total"),
                get_handle_null(release_date, "coming_soon"),
                get_handle_null(release_date, "date"),
                get_handle_null(support_info, "url"),
                get_handle_null(support_info, "email"),
                get_handle_null(data, "background"),
                get_handle_null(data, "background_raw"),
            ),
        )
        cursor.execute(query)

        # Query to update genres table if necessary
        genres = get_handle_null(data, "genres")
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
                        get_handle_null(genre, "id"),
                        get_handle_null(genre, "description"),
                        get_handle_null(genre, "id"),
                    ),
                )
                cursor.execute(query)

        # Query that inserts in the app_genres tables.
        if genres != None:
            print(f"genres found for app:{appid}")
            for genre in genres:
                query = cursor.mogrify(
                    """
                        INSERT INTO app_genres 
                        (
                            app_id,
                            genre_id
                        ) VALUES (%s, %s);""",
                    (appid, get_handle_null(genre, "id")),
                )
                cursor.execute(query)

        # Query that inserts into the dlc table if necissary.
        dlc = get_handle_null(data, "dlc")
        if dlc != None:
            print(f"dlc found for app:{appid}")
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
        developers = get_handle_null(data, "developers")
        if developers != None:
            print(f"developers found for app:{appid}")
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
        publishers = get_handle_null(data, "publishers")
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
        price = get_handle_null(data, "price_overview")
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
                    get_handle_null(price, "initial"),
                    get_handle_null(price, "final"),
                ),
            )
            cursor.execute(query)

        # Query that inserts into the packages table.
        packages = get_handle_null(data, "packages")
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

        # Query that inserts in the tags table.
        categories = get_handle_null(data, "categories")
        if categories != None:
            for tag in categories:
                query = cursor.mogrify(
                    """
                        INSERT INTO tags 
                        (
                            tag_id,
                            tag
                        )
                        SELECT %s, %s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM tags
                            WHERE tags.tag_id = %s
                        );
                    """,
                    (
                        get_handle_null(tag, "id"),
                        get_handle_null(tag, "description"),
                        get_handle_null(tag, "id"),
                    ),
                )
                cursor.execute(query)

        # Query that inserts in the app_tags tables.
        if categories != None:
            for tag in categories:
                query = cursor.mogrify(
                    """
                        INSERT INTO app_tags 
                        (
                            app_id,
                            tag_id
                        ) VALUES (%s, %s);
                    """,
                    (appid, get_handle_null(tag, "id")),
                )
                cursor.execute(query)

        # Query that inserts into the app_screenshots table
        screenshots = get_handle_null(data, "screenshots")
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
                        get_handle_null(screenshot, "id"),
                        get_handle_null(screenshot, "path_thumbnail"),
                        get_handle_null(screenshot, "path_full"),
                    ),
                )
                cursor.execute(query)

        # Query that inserts into the movies table.
        movies = get_handle_null(data, "movies")
        if movies != None:
            for movie in movies:
                webm = get_handle_null(movie, "webm")
                mp4 = get_handle_null(movie, "mp4")
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
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        appid,
                        get_handle_null(movie, "id"),
                        get_handle_null(movie, "name"),
                        get_handle_null(movie, "thumbnail"),
                        get_handle_null(webm, "480"),
                        get_handle_null(webm, "max"),
                        get_handle_null(mp4, "480"),
                        get_handle_null(mp4, "max"),
                        get_handle_null(movie, "highlight"),
                    ),
                )
                cursor.execute(query)

        # Query to add ratings to the ratings table.
        ratings = get_handle_null(data, "ratings")
        esrb = get_handle_null(ratings, "esrb")
        oflc = get_handle_null(ratings, "oflc")
        crl = get_handle_null(ratings, "crl")
        usk = get_handle_null(ratings, "usk")
        dejus = get_handle_null(ratings, "dejus")
        cero = get_handle_null(ratings, "cero")
        kgrb = get_handle_null(ratings, "kgrb")
        csrr = get_handle_null(ratings, "cssr")
        steam_germany = get_handle_null(ratings, "steam_germany")
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
                get_handle_null(esrb, "rating"),
                get_handle_null(esrb, "descriptors"),
                get_handle_null(esrb, "use_age_gate"),
                get_handle_null(esrb, "required_age"),
                get_handle_null(oflc, "rating"),
                get_handle_null(oflc, "descriptors"),
                get_handle_null(oflc, "use_age_gate"),
                get_handle_null(oflc, "required_age"),
                get_handle_null(crl, "rating"),
                get_handle_null(crl, "descriptors"),
                get_handle_null(crl, "use_age_gate"),
                get_handle_null(crl, "required_age"),
                get_handle_null(usk, "rating"),
                get_handle_null(usk, "descriptors"),
                get_handle_null(usk, "use_age_gate"),
                get_handle_null(usk, "required_age"),
                get_handle_null(usk, "rating_id"),
                get_handle_null(dejus, "rating"),
                get_handle_null(dejus, "descriptors"),
                get_handle_null(dejus, "use_age_gate"),
                get_handle_null(dejus, "required_age"),
                get_handle_null(cero, "rating"),
                get_handle_null(cero, "descriptors"),
                get_handle_null(kgrb, "rating"),
                get_handle_null(kgrb, "descriptors"),
                get_handle_null(csrr, "rating"),
                get_handle_null(csrr, "descriptors"),
                get_handle_null(steam_germany, "rating"),
                get_handle_null(steam_germany, "descriptors"),
                get_handle_null(steam_germany, "use_age_gate"),
                get_handle_null(steam_germany, "required_age"),
                get_handle_null(steam_germany, "banned"),
            ),
        )
        cursor.execute(query)

        # Query to update the landing transformed value.
        query = cursor.mogrify(
            """
                UPDATE steam_landing
                SET transformed = %s
                WHERE app_id = %s
            """,
            ("1", appid),
        )
        cursor.execute(query)

        # When Psycopg2 executes a query it does so as a transaction that does not complete until commit is run. Any failure before this should send an error that includes what appid specifically failed.
        conn.commit()

    conn.close()
