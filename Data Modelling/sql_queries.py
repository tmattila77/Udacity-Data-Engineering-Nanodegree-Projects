# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, start_time timestamp NOT NULL, user_id varchar UNIQUE, level text, song_id varchar, artist_id varchar, session_id int, location text, user_agent varchar);")

user_table_create = ("CREATE TABLE IF NOT EXISTS users (user_id varchar PRIMARY KEY NOT NULL, first_name text, last_name text, gender text, level text);")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY NOT NULL, title text NOT NULL, artist_id varchar NOT NULL, year int, duration decimal NOT NULL);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY NOT NULL, name text NOT NULL, location text, latitude numeric, longitude numeric);")

time_table_create = ("CREATE TABLE IF NOT EXISTS time (start_time bigint PRIMARY KEY NOT NULL, hour int, day int, week int, month text, year int, weekday text);")

# INSERT RECORDS

songplay_table_insert = ("INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING;")

user_table_insert = ("INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id;")

song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO UPDATE SET song_id = EXCLUDED.song_id;")

artist_table_insert = ("INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO UPDATE SET artist_id = EXCLUDED.artist_id;")


time_table_insert = ("INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO UPDATE SET start_time = EXCLUDED.start_time;")

#FIND SONGS

song_select = ("SELECT s.song_id, a.artist_id FROM songs s JOIN artists a ON s.artist_id = a.artist_id WHERE s.title = %s AND a.name = %s AND s.duration = %s")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]