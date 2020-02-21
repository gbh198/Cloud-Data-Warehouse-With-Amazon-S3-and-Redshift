import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE        = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')


# DROP TABLES IF EXISTS

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE STAGING TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
    event_ID               INT             NOT NULL   IDENTITY(0,1)  PRIMARY KEY,
    artist                 VARCHAR,
    auth                   VARCHAR,
    first_name             VARCHAR,
    gender                 VARCHAR,
    item_in_session        INTEGER,
    last_name              VARCHAR,
    length                 FLOAT, 
    level                  VARCHAR,
    location               VARCHAR,
    method                 VARCHAR,
    page                   VARCHAR,
    registration           VARCHAR,
    session_ID             BIGINT          NOT NULL                  SORTKEY DISTKEY,
    song_title             VARCHAR,
    status                 INTEGER,  
    ts                     VARCHAR,
    user_agent             TEXT,
    user_ID                VARCHAR
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
    song_ID             VARCHAR            NOT NULL         PRIMARY KEY,
    num_songs           INTEGER,
    artist_ID           VARCHAR                             SORTKEY DISTKEY,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER
    );
""")


# CREATE TABLES IN STAR SCHEMA

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
    songplay_ID    INTEGER         NOT NULL     IDENTITY(0,1)     PRIMARY KEY                       SORTKEY,
    start_time     TIMESTAMP                                      REFERENCES time(start_time),
    user_ID        VARCHAR(50)                                    REFERENCES users(user_ID)         DISTKEY,
    level          VARCHAR(10),
    song_ID        VARCHAR(50)                                    REFERENCES songs(song_ID),
    artist_ID      VARCHAR(50)                                    REFERENCES artists(artist_ID),
    session_ID     INTEGER,
    location       VARCHAR(255),
    user_agent     VARCHAR(255)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
    user_ID        VARCHAR(50)     NOT NULL                       PRIMARY KEY                       SORTKEY,
    first_name     VARCHAR(80),
    last_name      VARCHAR(80),
    gender         VARCHAR(1),
    level          VARCHAR(10)
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
    song_ID     VARCHAR(50)        NOT NULL                       PRIMARY KEY                       SORTKEY,
    title       VARCHAR(255),
    artist_ID   VARCHAR(50)        NOT NULL,
    year        INTEGER,
    duration    FLOAT
    )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
    artist_ID   VARCHAR(50)        NOT NULL                       PRIMARY KEY                       SORTKEY,
    name        VARCHAR(255),
    location    VARCHAR(255),
    latitude    FLOAT,
    longitude   FLOAT
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
    start_time  TIMESTAMP          NOT NULL                       PRIMARY KEY                       SORTKEY,
    hour        SMALLINT,
    day         SMALLINT,
    week        SMALLINT,
    month       SMALLINT,
    year        SMALLINT,
    weekday     SMALLINT
    )
    diststyle all;
""")


# LOADING FROM S3 INTO STAGING TABLES


staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    COMPUPDATE OFF
    FORMAT AS JSON '{}';
    """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)


staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    COMPUPDATE OFF 
    FORMAT AS JSON 'auto';
    """).format(SONG_DATA, IAM_ROLE)


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_ID, level, song_ID, artist_ID, session_ID, location, user_agent) 
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time, 
        e.user_ID, 
        e.level,
        s.song_ID,
        s.artist_ID,
        e.session_ID,
        e.location,
        e.user_agent
    FROM staging_events e, staging_songs s
    WHERE e.song_title = s.title
    AND e.page = 'NextSong';
""")
 
user_table_insert = ("""
    INSERT INTO users (user_ID, first_name, last_name, gender, level)  
    SELECT DISTINCT 
        user_ID,
        first_name,
        last_name,
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_ID, title, artist_ID, year, duration) 
    SELECT DISTINCT 
        song_ID, 
        title,
        artist_ID,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_ID, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_ID,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
        EXTRACT(hour from start_time) AS hour,
        EXTRACT(day from start_time) AS day,
        EXTRACT(week from start_time) AS week,
        EXTRACT(month from start_time) AS month,
        EXTRACT(year from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM staging_events      
    WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
