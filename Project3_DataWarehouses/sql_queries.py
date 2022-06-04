import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        first_name varchar,
        gender varchar,
        item_in_session integer,
        last_name varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        session_id int,
        song varchar,
        status integer,
        ts numeric,
        user_agent varchar,
        user_id int
)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs integer,
        artist_id varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year integer
)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id integer IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp without time zone NOT NULL,
        user_id integer NOT NULL,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar,
        session_id int NOT NULL,
        location varchar NOT NULL,
        user_agent varchar NOT NULL
)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer PRIMARY KEY, 
        first_name varchar NOT NULL, 
        last_name varchar NOT NULL, 
        gender varchar NOT NULL, 
        level varchar NOT NULL
)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY, 
        title varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        year int NOT NULL, 
        duration numeric NOT NULL
)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY, 
        name varchar NOT NULL,
        location varchar, 
        latitude numeric, 
        longitude numeric
)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp without time zone PRIMARY KEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as JSON {}
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE','ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as JSON 'auto'
""").format(config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE','ARN'),)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time, 
        e.user_id AS user_id, 
        e.level AS level, 
        s.song_id AS song_id, 
        s.artist_id AS artist_id, 
        e.session_id AS session_id, 
        e.location AS location, 
        e.user_agent AS user_agent
    FROM staging_events e
    INNER JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        e.user_id AS user_id, 
        e.first_name AS first_name, 
        e.last_name AS last_name, 
        e.gender AS gender, 
        e.level AS level
    FROM staging_events e
    WHERE e.page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        s.song_id AS song_id, 
        s.title AS title, 
        s.artist_id AS artist_id, 
        s.year AS year, 
        s.duration AS duration
    FROM staging_songs s
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        s.artist_id AS artist_id, 
        s.artist_name AS name, 
        s.artist_location AS location, 
        s.artist_latitude AS latitude, 
        s.artist_longitude AS longitude
    FROM staging_songs s
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        t.start_time AS start_time, 
        EXTRACT (HOUR FROM t.start_time) AS hour, 
        EXTRACT (DAY FROM t.start_time) AS day, 
        EXTRACT (WEEK FROM t.start_time) AS week, 
        EXTRACT (MONTH FROM t.start_time) AS month, 
        EXTRACT (YEAR FROM t.start_time) AS year, 
        EXTRACT (WEEKDAY FROM t.start_time) AS weekday
    FROM (
        SELECT DISTINCT timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time
        FROM staging_events e
        WHERE e.page = 'NextSong') t
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
