import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    """
    Drop existing tables in Redshift database if the function is called.
    Key arguments: 
    cur: A cursor to perform operations in the database
    conn: Connection variable to the Redshift database.
    
    Output: The existing datatables in the Redshift database are deleted.
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        print('table dropped successfully')
        conn.commit()
    


def create_tables(cur, conn):
    
    """
    Create the fact and dimension tables in Redshift database if the function is called.
    Key arguments: 
    cur: A cursor to perform operations in the database
    conn: Connection variable to the Redshift database.
    
    Output: The fact and dimension datatables (specified in sql_queries.py) in the Redshift database are created.
    """
    for query in create_table_queries:
        cur.execute(query)
        print('table created successfully')
        conn.commit()


def main():
    """
    Connect to the Redshift database, create a cursor to be able to make changes in the database.
    Call drop_tables and create_tables functions to drop existing tables and create new fact and dimension tables. 
    (Tables are spcified in sql_queries.py)
    After carrying out those tasks, connection to the database will be closed.
    """
    #Clusteridentifier, names of the database and the user, password and port are retrieved from dwh.cfg config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    host = config.get('CLUSTER', 'HOST')
    dbname = config.get('CLUSTER', 'DB_NAME')
    user = config.get('CLUSTER', 'DB_USER')
    password = config.get('CLUSTER', 'DB_PASSWORD')
    port = config.get('CLUSTER', 'DB_PORT')
    
    
    #Connection will be built up and cursor will be defined.
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, dbname, user,
                                                                                   password, port))
    cur = conn.cursor()

    #drop_tables and create_tables functions (above) will be called.
    drop_tables(cur, conn)
    create_tables(cur, conn)

    #connection to the database will be closed.
    conn.close()


if __name__ == "__main__":
    main()