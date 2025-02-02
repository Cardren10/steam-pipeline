CREATE TABLE steam_landing (
    id INT PRIMARY KEY SERIAL,
    app_id INT,
    app_data JSONB,
    source CHARACTER(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transformed BOOLEAN,
);

CREATE TABLE apps (
    id INT PRIMARY KEY,
    app_name CHARACTER(255),
    app_type CHARACTER(255),
    genreid INT FOREIGN KEY,
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
    platform_linux BOOLEAN
);

CREATE TABLE app_genres (
    app_id INT FOREIGN KEY,
    genre_id INT FOREIGN KEY
);

CREATE TABLE genres (
    genre_id INT PRIMARY KEY,
    genre CHARACTER(255)
);

CREATE TABLE dlcs (
    app_id INT FOREIGN KEY,
    dlc_id INT
);

CREATE TABLE developers (
    app_id INT FOREIGN KEY,
    developer CHARACTER(255)
);

CREATE TABLE publishers (
    app_id INT FOREIGN KEY,
    publisher CHARACTER(255)
);

CREATE TABLE prices (
    app_id INT FOREIGN KEY,
    currency CHARACTER(12),
    initial INT,
    final INT
);

CREATE TABLE packages (
    app_id INT FOREIGN KEY,
    package_id INT
);

CREATE TABLE app_tags (
    app_id INT FOREIGN KEY,
    tag_id INT FOREIGN KEY
);

CREATE TABLE tags (
    tag_id INT PRIMARY KEY,
    tag CHARACTER(255)
);

CREATE TABLE app_screenshots (
    app_id INT FOREIGN KEY,
    app_screenshots_id INT,
    path_thumbnail TEXT,
    path_full TEXT
)

CREATE TABLE movies (
    app_id INT FOREIGN KEY,
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
    app_id INT FOREIGN KEY,
    esrb_rating CHARACTER(8),
    esrb_descriptors TEXT,
    esrb_age_gate CHARACTER(8),
    esrb_required_age CHARACTER(8),
    oflc_rating CHARACTER(8),
    oflc_descriptors TEXT,
    oflc_age_gate CHARACTER(8),
    oflc_required_age CHARACTER(8),
    crl_rating CHARACTER(8),
    crl_descriptors TEXT,
    crl_age_gate CHARACTER(8),
    crl_required_age CHARACTER(8),
    usk_rating CHARACTER(8),
    usk_descriptors TEXT,
    usk_age_gate CHARACTER(8),
    usk_required_age CHARACTER(8),
    usk_rating_id CHARACTER(8),
    dejus_rating CHARACTER(8),
    dejus_descriptors TEXT,
    dejus_age_gate CHARACTER(8),
    dejus_required_age CHARACTER(8),
    cero_rating CHARACTER(8),
    cero_descriptors TEXT,
    kgrb_rating CHARACTER(8),
    kgrb_descriptors TEXT,
    csrr_rating CHARACTER(8),
    csrr_descriptors TEXT,
    steam_germany_rating CHARACTER(8),
    steam_germany_descriptors TEXT,
    steam_germany_age_gate CHARACTER(8),
    steam_germany_required_age CHARACTER(8),
    steam_germany_banned CHARACTER(8),

);