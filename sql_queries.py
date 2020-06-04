# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)
        
#1.Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
print("Query 1:")
print("artist | song | length")
try:
    session.set_keyspace('sparkifydb')
except Exception as e:
    print(e)

query = "SELECT artist, song, length FROM songplay_session WHERE session_id = 338 and item_in_session=4"
try:
    rows = session.execute(query)
    for row in rows:
        print (row.artist, "|", row.song, "|", row.length)
except Exception as e:
    print(e)
    
#2.  Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
print("Query 2:")
print("artist | song | first_name | last_name | user_id | item_in_session")
try:
    session.set_keyspace('sparkifydb')
except Exception as e:
    print(e)

query = "SELECT artist, song, first_name, last_name, user_id, item_in_session \
                    FROM artist_session \
                    WHERE session_id = 182 AND user_id = 10"
try:
    rows = session.execute(query)
    for row in rows:
        print (row.artist, '|', row.song, '|', row.first_name, '|', row.last_name, '|', row.item_in_session)
except Exception as e:
    print(e)

#3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
print("Query 3:")
print("first_name, last_name")
query = "SELECT first_name, last_name, user_id \
            FROM user_song  \
            WHERE song = 'All Hands Against His Own' " 
try:
    rows = session.execute(query)
    for row in rows:
        print (row.first_name, row.last_name)
except Exception as e:
    print(e)


session.shutdown()
cluster.shutdown()