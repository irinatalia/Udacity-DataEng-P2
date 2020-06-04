# DROP TABLES
# The following CQL queries drop all the tables from sparkifydb.
songplay_session_table_drop = "DROP TABLE IF EXISTS songplay_session"
artist_session_table_drop = "DROP TABLE IF EXISTS artist_session"
user_song_table_drop = "DROP TABLE IF EXISTS user_song"

# CREATE TABLES
songplay_session_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_session (
        item_in_session int, 
        session_id int,        
        artist text, 
        song text, 
        length float, 
        PRIMARY KEY(session_id, item_in_session)
        )
""")

artist_session_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_session (
        user_id int, 
        session_id int,
        item_in_session int,                
        artist text, 
        song text, 
        first_name text, \
        last_name text, \
        PRIMARY KEY((user_id, session_id), item_in_session)
    )
""")

user_song_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_song (
        user_id int,
        song text, 
        first_name text, 
        last_name text, 
        PRIMARY KEY(song, user_id)
    )
""")

# INSERT RECORDS
# query to insert data into songplay_session table
songplay_session_insert = ("""
    INSERT INTO songplay_session (  item_in_session, 
                                    session_id, 
                                    artist, 
                                    song, 
                                    length)
    VALUES (%s, %s, %s, %s, %s)
""")

artist_session_insert = ("""
    INSERT INTO artist_session ( user_id,
                                    session_id, 
                                    item_in_session,
                                    artist, 
                                    song,
                                    first_name, 
                                    last_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

user_song_insert = ("""
    INSERT INTO user_song  (user_id, 
                            song,
                            first_name, 
                            last_name)
    VALUES (%s, %s, %s, %s)
""")

# SELECT QUERIES
# The follwowing queries select data from DB.
#
# Query-1: 1. Give me the artist, song title and song's length in the music app history 
# that was heard during sessionId = 338, and itemInSession = 4
songplay_session_select = ("""
    SELECT artist, song, length
                FROM songplay_session 
                WHERE   session_id = (%s) AND item_in_session = (%s)
""")
#
# Query-2: Give me only the following: name of artist,
# song (sorted by itemInSession) and user (first and last name)
# for userid = 10, sessionid = 182
artist_session_select = ("""
    SELECT artist, song, first_name, last_name
            FROM artist_session 
            WHERE   session_id = (%s) AND 
                    item_in_session = (%s)
""")
#
# Query-3: Give me every user name (first and last) in my music app
# history who listened to the song 'All Hands Against His Own'
user_song_select = ("""
    SELECT first_name, last_name
            FROM user_song 
            WHERE song = (%s)
""")
#QUERY LISTS
create_table_queries = [songplay_session_table_create, artist_session_table_create, user_song_table_create]
drop_table_queries = [songplay_session_table_drop, artist_session_table_drop, user_song_table_drop]
