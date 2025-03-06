CREATE TABLE apps (
    app_id INT PRIMARY KEY,
    app_name CHARACTER(255),
    app_type CHARACTER(255),
    required_age INT,
    is_free BOOLEAN,
    controller_support CHARACTER(255),
    detailed_description TEXT,
    about_the_game TEXT,
    short_description TEXT,
    supported_languages TEXT,
    reviews TEXT,
    header_image TEXT,
    capsule_image TEXT,
    capsule_imagev5 TEXT,
    website TEXT,
    pc_requirements_minimum TEXT,
    pc_requirements_recommended TEXT,
    mac_requirements_minimum TEXT,
    mac_requirements_recommended TEXT,
    linux_requirements_minimum TEXT,
    linux_requirements_recommended TEXT,
    legal_notice TEXT,
    platform_windows BOOLEAN,
    platform_mac BOOLEAN,
    platform_linux BOOLEAN,
    total_achievements INT,
    coming_soon BOOLEAN,
    release_date TEXT,
    support_url CHARACTER(255),
    support_email CHARACTER(255),
    background TEXT,
    background_raw TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);

CREATE TABLE steam_landing (
    id SERIAL PRIMARY KEY,
    app_id INT,
    app_data JSONB,
    app_source CHARACTER(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transformed BOOLEAN
);

CREATE TABLE genres (
    genre_id INT PRIMARY KEY,
    genre CHARACTER(255)
);

CREATE TABLE app_genres (
    app_id INT,
    genre_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE dlcs (
    app_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    dlc_id INT
);

CREATE TABLE developers (
    app_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    developer CHARACTER(255)
);

CREATE TABLE publishers (
    app_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    publisher CHARACTER(255)
);

CREATE TABLE prices (
    app_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    currency CHARACTER(12),
    initial INT,
    final_price INT
);

CREATE TABLE packages (
    app_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    package_id INT
);

CREATE TABLE tags (
    tag_id INT PRIMARY KEY,
    tag CHARACTER(255)
);

CREATE TABLE app_tags (
    app_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    tag_id INT,
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);

CREATE TABLE app_screenshots (
    app_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    app_screenshots_id INT,
    path_thumbnail TEXT,
    path_full TEXT
);

CREATE TABLE movies (
    app_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    movie_id INT PRIMARY KEY,
    movie_name CHARACTER(255),
    thumbnail TEXT,
    webm_480 TEXT,
    webm_max TEXT,
    mp4_480 TEXT,
    mp4_max TEXT,
    highlight BOOLEAN
);

CREATE TABLE ratings (
    app_id INT,
    FOREIGN KEY (app_id) REFERENCES apps(app_id),
    esrb_rating TEXT,
    esrb_descriptors TEXT,
    esrb_age_gate TEXT,
    esrb_required_age TEXT,
    oflc_rating TEXT,
    oflc_descriptors TEXT,
    oflc_age_gate TEXT,
    oflc_required_age TEXT,
    crl_rating TEXT,
    crl_descriptors TEXT,
    crl_age_gate TEXT,
    crl_required_age TEXT,
    usk_rating TEXT,
    usk_descriptors TEXT,
    usk_age_gate TEXT,
    usk_required_age TEXT,
    usk_rating_id TEXT,
    dejus_rating TEXT,
    dejus_descriptors TEXT,
    dejus_age_gate TEXT,
    dejus_required_age TEXT,
    cero_rating TEXT,
    cero_descriptors TEXT,
    kgrb_rating TEXT,
    kgrb_descriptors TEXT,
    csrr_rating TEXT,
    csrr_descriptors TEXT,
    steam_germany_rating TEXT,
    steam_germany_descriptors TEXT,
    steam_germany_age_gate TEXT,
    steam_germany_required_age TEXT,
    steam_germany_banned TEXT
);