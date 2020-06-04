import cassandra
from cql_queries import create_table_queries, drop_table_queries


def create_database():
    """Connect to Apache Cassandra
    drop any existing sparkifydb and create a new instence.

    Input:
    * None

    Output:
    * session connection
    """
    # connect to default database
    from cassandra.cluster import Cluster
    try:
        # Connect to a local Cassandra cluster
        cluster = Cluster(['127.0.0.1'])
        # Set a session execute queries.
        session = cluster.connect()

    except Exception as e:
        print(e)

    # drop sparkify keyspace
    try:
        session.execute("""
            DROP KEYSPACE IF EXISTS sparkifydb
        """)
    except Exception as e:
        print(e)

    # create sparkify keyspace
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS sparkifydb
            WITH REPLICATION =
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """)
    except Exception as e:
        print(e)

    try:
        # Set KEYSPACE to the keyspace specified above
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)

    return cluster, session


def drop_tables(session):
    """Drop any existing tables from sparkifydb.

    Input:
    * session -- connection to sparkifydb keyspace

    Output:
    * Old sparkifydb database tables are dropped 
    """
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

    print("Tables dropped successfully.")

def create_tables(session):
    """Creates new tables in sparkifydb.

    Input
    * session -- connection to sparkifydb keyspace

    Output:
    * Creates new sparkifydb database tables
    """
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)
    print("Tables created successfully.")

def main():
    """Connect to Apache Cassandra, create new sparkifydb

    Input:
    * None

    Output:
    * Creates new sparkifydb database tables
    """
    # Connect to DB, drop old DB, create new DB. Return session to DB.
    cluster, session = create_database()

    # Drop old tables from DB.
    drop_tables(session)
    # Create new tables to DB.
    create_tables(session)

    # Close the session and DB connection.
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
