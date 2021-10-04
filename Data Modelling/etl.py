import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
from datetime import datetime

def process_song_file(cur, filepath):
    
    """
    Description: This function is to open files in 'data/song_data' as well as load data into SONG TABLE \
    and ARTIST TABLE.
    
    Arguments: 
    cur: the cursor object
    filepath: file path to song data
    
    Returns: None
    """
    # open song file
    with open(filepath) as song_json:
        df = pd.read_json(song_json, lines = True)

    # insert song record
    for i in range(df.shape[0]):
        song_data = song_data =df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[i].tolist()
        cur.execute(song_table_insert, song_data)
    
    # insert artist record
    for j in range(df.shape[0]):
        artist_data = df[['artist_id', 'artist_name','artist_location','artist_latitude','artist_longitude']].values[j].tolist()
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function is used to open files in 'data/log_data' as well as load data into the tables as follows:\
    TIME, USER, SONGPLAYS
    
    Arguments:
    cur: the cursor object
    filepath: file path to log_data
    
    Returns: None
    """
    
    
    # open log file
    with open(filepath) as log_json:
        df = pd.read_json(log_json, lines = True)

    # filter by NextSong action
    for index, value in enumerate(df['page']):
        if value != 'NextSong':
            df.drop(index = index, inplace = True)
    

    # convert timestamp column to datetime
    dtime = []
    for ts in df['ts']:
        dt = datetime.fromtimestamp(ts/1000)
        dtime.append(dt)
        
    df['datetime'] = dtime
        
    hour = df['datetime'].dt.hour
    day = df['datetime'].dt.day
    week_of_year = df['datetime'].dt.weekofyear
    month = df['datetime'].dt.month
    year = df['datetime'].dt.year
    weekday = df['datetime'].dt.weekday
        
     
    
    # insert time data records
    
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year','weekday' ]
    time_dict = {column_labels[0]:df['ts'], column_labels[1]:hour, column_labels[2]:day, column_labels[3]:week_of_year, \
                 column_labels[4]:month, column_labels[5]:year, column_labels[6]: weekday}
    time_df = pd.DataFrame(data = time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    # load user table
    user_df = user_df = pd.DataFrame({'user_id': df['userId'], 'first_name':df['firstName'], 'last_name':df['lastName'],\
                                      'gender': df['gender'], 'level': df['level']})

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is used to access files in 'data/song_data' and 'data/log_data' and to call process functions.
    
    Arguments: 
    cur: The cursor object
    conn: The connection object
    filepath: the file path to the data files
    func: the process functions that will be called
    
    Returns: None
    
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description: main function to run the script in the command line, to create connection and cursor objects and to call process\
    functions
    
    Arguments: None
    
    Returns: None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()