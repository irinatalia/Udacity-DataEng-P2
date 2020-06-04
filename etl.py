# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cql_queries import *

def process_song_file(session, filepath):
    """Process input data file and insert data into tables.
    INPUTS:
    * session --    reference to connected db.
    * filepath --   path to CSV file to be processed.

    Output:
    * added data to tables
    """
    print('Inserting data to songplay_session table.')    
    # Process songplay_session table.
    lines_count_1 = 0
    with open(filepath, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(songplay_session_insert, \
                            (int(line[3]), \
                            int(line[8]), \
                            line[0], \
                            line[9], \
                            float(line[5])))
            lines_count_1 += 1
        print('Data inserted successfully into songplay_session table. Number of records inserted: ', lines_count_1 )

    # Process artist_session_insert table.
    print('Inserting data to artist_session table.')    
    lines_count_2 = 0
    with open(filepath, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(artist_session_insert, \
                            (int(line[10]), \
                            int(line[8]), \
                            int(line[3]), \
                            line[0], \
                            line[9], \
                            line[1], \
                            line[4]))
            lines_count_2 += 1
        print('Data inserted successfully into artist_session table. Number of records inserted: ', lines_count_2)
            
    # Process songplay_session table.
    print('Inserting data to user_song table.')    
    lines_count_3 = 0
    with open(filepath, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(user_song_insert, \
                            (int(line[10]), \
                            line[9], \
                            line[1], \
                            line[4] ))
            lines_count_3 += 1 
        print('Data inserted successfully into user_song table. Number of records inserted: ', lines_count_3)
        
def process_data(session, filepath, data_file, func):
    """
    combine all files in one and get the data from the combined file. 
    Upload data from combined file into tables.

    INPUTS:
    * session --    session to keyspace (sparkifydb)
    * filepath --   path to file to be processed
    * data_file --  file that has all data combined
    * func --       function to be called (process_song_data or
                    process_log_data)
    OUTPUT:
    * console printouts of the data processing.
    """
    file_path_list = []
    for root, dirs, files in os.walk(filepath):
        for f in files :
            file_path_list.append(os.path.abspath(f))
            print('Adding to filepath list: ' + f)

        print('Total number of files: ', len(file_path_list)) 

        file_path_list = glob.glob(os.path.join(root,'*'))
        #print(file_path_list)

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:

        # reading csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line)

    print('Total number of rows: ', len(full_data_rows_list))
    
    # creating a smaller csv file called event_datafile_new
    # csv that will be used to insert data into the database    
    csv.register_dialect(   'myDialect', 
                            quoting=csv.QUOTE_ALL, 
                            skipinitialspace=True)

    with open(data_file, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender', 'itemInSession','lastName','length', 
                    'level','location','sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], 
                            row[6], row[7], row[8], row[12], row[13], row[16]))
        print('Input data file: ' + data_file)

    # Insert pre-processed data from single CSV to Apache Cassandra DB.
    func(session, data_file)
    print('All input data processed and inserted to DB.')


def main():
    """
    Connect to DB, drop tables if they exist, create new tables, load data from (./event_data/*.csv).
    INPUTS:
    * None

    OUTPUT:
    * All input data is processed in DB tables.
    """

    # Make a connection to a Cassandra instance
    # (127.0.0.1)
    from cassandra.cluster import Cluster
    try: 
        cluster = Cluster(['127.0.0.1']) 
        session = cluster.connect()
    except Exception as e:
        print(e)

    try:
        # Set keyspace (sparkifydb) for Cassandra session.
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)

    # Get current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'
    data_file = 'event_datafile_new.csv'

    process_data( session, filepath, data_file, func=process_song_file)

    # Close the session and DB connection.
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
