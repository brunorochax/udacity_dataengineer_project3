"""This is the etl module.
It will be responsible for import and load all tables in star schema.
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This is the load staging tables function,
    it will be responsabile for load the staging tables.
    
    Args:
        cur (psycopg2.extensions.cursor): The cursor from your connection with redshift
        conn (psycopg2.extensions.connection): The connection with redshift
    Returns:
        None
    """

    for query in copy_table_queries:
        print('Executing: ' + query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This is the insert tables function,
    it will be responsabile for load the final tables on start schema.
    
    Args:
        cur (psycopg2.extensions.cursor): The cursor from your connection with redshift
        conn (psycopg2.extensions.connection): The connection with redshift
    Returns:
        None
    """

    for query in insert_table_queries:
        print('Executing: ' + query)
        cur.execute(query)
        conn.commit()


def main():
    """This is the main function, will start with this module
    and execute all another functions.
    
    Args:
        None
        
    Returns:
        None
    """
    
    config = configparser.ConfigParser()
    config.read('sparkify.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        print('Loading staging tables...')
        load_staging_tables(cur, conn)

        print('Inserting data on final tables...')
        insert_tables(cur, conn)

        conn.close()        
        print('Tables have been loaded.')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()