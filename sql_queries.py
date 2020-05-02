import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist varchar, auth varchar, first_name varchar, gender varchar, item_session int, last_name varchar, length numeric(10,5), level varchar, location text, method varchar, page varchar, registration float, session_id int, song text, status int, ts bigint, user_agent text, user_id varchar)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs int, artist_id varchar, artist_latitude decimal(9,6), artist_longitude decimal(9,6), artist_location text, artist_name varchar, song_id varchar, title text, duration numeric(10,5), year int)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(1,1), start_time timestamp NOT NULL, user_id varchar NOT NULL, level varchar, song_id varchar NOT NULL, artist_id varchar NOT NULL, session_id int, location text, user_agent text, PRIMARY KEY (songplay_id))""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id varchar, first_name varchar, last_name varchar, gender char(1), level varchar, PRIMARY KEY (user_id))""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar, title text, artist_id varchar NOT NULL, year int, duration numeric(10,5), PRIMARY KEY (song_id))""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar, name varchar, location text, latitude decimal(9,6), longitude decimal(9,6), PRIMARY KEY (artist_id))""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour int, day int, week int, month int, year int, weekday int,PRIMARY KEY (start_time))""")

# STAGING TABLES

# LOAD LOGS AND SONGS JSON FILE FROM S3 BUCKET
# COPY JSON FILE TO REDSWIFT STAGING TABLE

staging_events_copy = ("""
copy staging_events from {}
iam_role {}
region 'us-west-2'
json {};
        """).format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
iam_role {}
region 'us-west-2'
format as json 'auto';
        """).format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, \
                                                    session_id, location, user_agent)
                            SELECT DISTINCT timestamp 'epoch' + se.ts / 1000 * interval '1 second' AS start_time,
                            se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
                            FROM staging_events se, staging_songs ss
                            WHERE se.artist = ss.artist_name
                            AND   se.song   = ss.title
                            AND   se.length = ss.duration
                            AND   se.page = 'NextSong'
                            AND   se.user_id NOT IN (SELECT DISTINCT s.user_id FROM songplays s WHERE s.user_id = se.user_id
                            AND   s.start_time = start_time AND s.session_id = se.session_id )
                            """)

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id, first_name, last_name, gender, level
                        FROM staging_events
                        WHERE page = 'NextSong'
                        AND user_id NOT IN (SELECT DISTINCT user_id FROM users)""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
                        FROM staging_events se, staging_songs ss
                        WHERE se.artist = ss.artist_name
                        AND   se.song   = ss.title
                        AND   se.length = ss.duration
                        AND   se.page = 'NextSong'
                        AND   ss.song_id NOT IN (SELECT DISTINCT song_id FROM songs)""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, \
                          ss.artist_latitude, ss.artist_longitude
                          FROM staging_events se, staging_songs ss
                          WHERE se.artist = ss.artist_name
                          AND   se.song   = ss.title
                          AND   se.length = ss.duration
                          AND   se.page = 'NextSong'
                          AND   ss.artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time,
                        EXTRACT(hour FROM start_time)                          AS hour,
                        EXTRACT(day FROM start_time)                           AS day,
                        EXTRACT(week FROM start_time)                          AS week,
                        EXTRACT(month FROM start_time)                         AS month,
                        EXTRACT(year FROM start_time)                          AS year,
                        EXTRACT(weekday FROM start_time)                       AS weekday
                        FROM staging_events
                        WHERE page = 'NextSong'
                        AND start_time NOT IN (SELECT DISTINCT start_time FROM time)""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
