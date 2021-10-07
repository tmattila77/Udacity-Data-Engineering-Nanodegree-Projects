import configparser


# CONFIG: access and read the config file dwh.cfg
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (artist varchar, auth varchar, firstName text, gender text, itemInSession int, lastName text, length decimal, level text, location text, method text, page varchar, registration numeric, sessionId int, song varchar, status bigint, ts bigint, userAgent varchar, user_id int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (num_songs int PRIMARY KEY UNIQUE, artist_id varchar, artist_latitude numeric, artist_longitude numeric, artist_location text, artist_name text, song_id varchar, title text, duration decimal, year int);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY, start_time timestamp, user_id varchar UNIQUE, level text, song_id varchar, artist_id varchar, session_id int, location text, user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id varchar PRIMARY KEY, first_name text, last_name text, gender text, level text);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title text, artist_id varchar, year int, duration decimal);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name text, location text, latitude numeric, longitude numeric);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour int, day int, week int, month text, year int, weekday text);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table FROM {} CREDENTIALS 'aws_iam_role={}' json {}
    """).format(config.get('S3', 'LOG_DATA'),
             config.get('IAM_ROLE', 'ARN'),
             config.get('S3', 'LOG_JSONPATH'))
staging_songs_copy = ("""
    COPY staging_songs_table FROM {} CREDENTIALS 'aws_iam_role={}' json 'auto'
    """).format(config.get('S3', 'SONG_DATA'),
             config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000* INTERVAL '1 second' AS start_time,   
e.user_id AS user_id, e.level AS level, s.song_id AS song_id, s.artist_id AS artist_id, e.sessionId AS session_id, 
e.location AS location, e.userAgent AS agent
FROM staging_events_table e JOIN staging_songs_table s ON e.artist = s.artist_name
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT DISTINCT user_id, firstName, lastName, gender, level FROM staging_events_table
WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs_table;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs_table;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000* INTERVAL '1 second' AS start_time,
        EXTRACT (hour FROM start_time) AS hour,
        EXTRACT (day FROM start_time) AS day,
        EXTRACT (week FROM start_time) AS week,
        EXTRACT (month FROM start_time) AS month,
        EXTRACT (year FROM start_time) AS year,
        EXTRACT (weekday FROM start_time) AS weekday
FROM staging_events_table e
WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
