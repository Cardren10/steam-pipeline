from helpers import db_conn
from helpers import constants


def load_data() -> None:
    conn = db_conn()
    cursor = conn.cursor()
    query = cursor.mogrify("""SELECT id, data FROM %s WHERE transformed = 0"""), (
        {constants.DATABASE_LANDING}
    )
    cursor.execute(query)
    rows = cursor.rowcount
    for i in range(rows):
        record = cursor.fetchone()
        appid = record[0]
        app = record[1]
        data = app.get(f"{appid}")

        # Query that inserts into the apps table.
        query = cursor.mogrify(
            """
                INSERT INTO apps 
                (
                    id,
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
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
            (
                appid,
                data.get("name"),
                data.get("type"),
                data.get("required_age"),
                data.get("is_free"),
                data.get("controller_support"),
                data.get("detailed_description"),
                data.get("about_the_game"),
                data.get("short_description"),
                data.get("supported_languages"),
                data.get("reviews"),
                data.get("header_image"),
                data.get("capsule_image"),
                data.get("capsule_imagev5"),
                data.get("website"),
                data.get("pc_requirements").get("minimum"),
                data.get("pc_requirements").get("recommended"),
                data.get("mac_requirements").get("minimum"),
                data.get("mac_requirements").get("recommended"),
                data.get("linux_requirements").get("minimum"),
                data.get("linux_requirements").get("recommended"),
                data.get("legal_notice"),
                data.get("platforms").get("windows"),
                data.get("platforms").get("mac"),
                data.get("platforms").get("linux"),
                data.get("achievements").get("total"),
                data.get("release_date").get("coming_soon"),
                data.get("release_date").get("date"),
                data.get("support_info").get("url"),
                data.get("support_info").get("email"),
                data.get("background"),
                data.get("background_raw"),
            ),
        )
        cursor.execute(query)

        # Query that inserts in the app_genres tables.
        genres = data.get("genres")
        for genre in genres:
            query = cursor.mogrify(
                """
                    INSERT INTO app_genres 
                    (
                        app_id,
                        genre_id
                    ) VALUES (%s, %s);""",
                (appid, genre.get("id")),
            )
            cursor.execute(query)

        # Query to update genres table if necessary
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
                (genre.get("id"), genre.get("description"), genre.get("id")),
            )
            cursor.execute(query)

        # Query that inserts into the dlc table if necissary.
        if len(data.get("dlc")) > 0:
            for dlc_id in data.get("dlc"):
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
        developers = data.get("developers")
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
        publishers = data.get("publishers")
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
        price = data.get("price_overview")
        query = cursor.mogrify(
            """
                INSERT INTO prices
                (
                    app_id,
                    initial,
                    final
                )
                VALUES (%s, %s, %s);
            """,
            (appid, price.get("initial"), price.get("final")),
        )
        cursor.execute(query)

        # Query that inserts into the packages table.
        packages = data.get("packages")
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

        # Query that inserts in the app_tags tables.
        categories = data.get("categories")
        for tag in categories:
            query = cursor.mogrify(
                """
                    INSERT INTO app_tags 
                    (
                        app_id,
                        tag_id
                    ) VALUES (%s, %s);
                """,
                (appid, tag.get("id")),
            )
            cursor.execute(query)

        # Query that inserts in the tags table.
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
                (tag.get("id"), tag.get("description"), tag.get("id")),
            )
            cursor.execute(query)

        # Query that inserts into the app_screenshots table
        screenshots = data.get("screenshots")
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
                    screenshot.get("id"),
                    screenshot.get("path_thumbnail"),
                    screenshot.get("path_full"),
                ),
            )
            cursor.execute(query)

        # Query that inserts into the movies table.
        movies = data.get("movies")
        for movie in movies:
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
                    movies.get("id"),
                    movies.get("name"),
                    movies.get("thumbnail"),
                    movies.get("webm").get("480"),
                    movies.get("webm").get("max"),
                    movies.get("mp4").get("480"),
                    movies.get("mp4").get("max"),
                    movies.get("highlight"),
                ),
            )
            cursor.execute(query)

        # Query to add ratings to the ratings table.
        ratings = data.get("ratings")
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
                ratings.get("esrb").get("rating"),
                ratings.get("esrb").get("descriptors"),
                ratings.get("esrb").get("use_age_gate"),
                ratings.get("esrb").get("required_age"),
                ratings.get("oflc").get("rating"),
                ratings.get("oflc").get("descriptors"),
                ratings.get("oflc").get("use_age_gate"),
                ratings.get("oflc").get("required_age"),
                ratings.get("crl").get("rating"),
                ratings.get("crl").get("descriptors"),
                ratings.get("crl").get("use_age_gate"),
                ratings.get("crl").get("required_age"),
                ratings.get("usk").get("rating"),
                ratings.get("usk").get("descriptors"),
                ratings.get("usk").get("use_age_gate"),
                ratings.get("usk").get("required_age"),
                ratings.get("usk").get("rating_id"),
                ratings.get("dejus").get("rating"),
                ratings.get("dejus").get("descriptors"),
                ratings.get("dejus").get("use_age_gate"),
                ratings.get("dejus").get("required_age"),
                ratings.get("cero").get("rating"),
                ratings.get("cero").get("descriptors"),
                ratings.get("kgrb").get("rating"),
                ratings.get("kgrb").get("descriptors"),
                ratings.get("csrr").get("rating"),
                ratings.get("csrr").get("descriptors"),
                ratings.get("steam_germany").get("rating"),
                ratings.get("steam_germany").get("descriptors"),
                ratings.get("steam_germany").get("use_age_gate"),
                ratings.get("steam_germany").get("required_age"),
                ratings.get("steam_germany").get("banned"),
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
