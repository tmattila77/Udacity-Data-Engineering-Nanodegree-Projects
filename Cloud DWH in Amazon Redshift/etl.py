import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load log_data and song_data out of the suitable S3 buckets and store it in staging tables from which dimension
    and fact tables
    will be populated.
    Key arguments: 
    cur: A cursor to perform operations in the database
    conn: Connection variable to the Redshift database.
    
    Output: Staging tables will be populated by the data pulled from the buckets.
    """
       
    print('Loading staging tables')
    for query in copy_table_queries:
        print("Executing query: {} ".format(query))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load and transform data from the staging tables and populate fact and dimension tables.
    Key arguments: 
    cur: A cursor to perform operations in the database
    conn: Connection variable to the Redshift database.
    
    Output: Dimension and fact tables are populated with the transformed data from the staging tables.
    """
    print('Inserting fact and dimension tables')
    for query in insert_table_queries:
        print("Executing query: {}".format(query))
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to the Redshift database, create a cursor to be able to make changes in the database.
    Call drop_tables and create_tables functions to drop existing tables and create new fact and dimension tables. 
    (Tables are spcified in sql_queries.py)
    After carrying out those tasks, connection to the database will be closed.
    """
    # Host, database name, username, password and port will be retrieved from the config file.
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    host = config.get('CLUSTER', 'HOST')
    dbname = config.get('CLUSTER', 'DB_NAME')
    user = config.get('CLUSTER', 'DB_USER')
    password = config.get('CLUSTER', 'DB_PASSWORD')
    port = config.get('CLUSTER', 'DB_PORT')
    
    
    #Connection will be built up and cursor will be defined.
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, dbname, user, password,
                                                                                   port))
    cur = conn.cursor()
    
    # Call the functions to laod staging tables and populate fact and dimension tables.
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    #connection to the database will be closed.
    conn.close()


if __name__ == "__main__":
    main()