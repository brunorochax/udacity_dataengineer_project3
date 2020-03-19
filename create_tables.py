"""This is the create_tables module.
It will be responsible for create all needed tables.
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """This is the drop table function,
    it will be responsabile for drop all tables if already exists.
    
    Args:
        cur (psycopg2.extensions.cursor): The cursor from your connection with redshift
        conn (psycopg2.extensions.connection): The connection with redshift
    Returns:
        None
    """
    for query in drop_table_queries:
        print('Executing: ' + query)
        cur.execute(query)
        conn.commit()

        
def create_tables(cur, conn):
    """This is the create table function,
    it will be responsabile for create all needed tables.
    
    Args:
        cur (psycopg2.extensions.cursor): The cursor from your connection with redshift
        conn (psycopg2.extensions.connection): The connection with redshift
    Returns:
        None
    """
    for query in create_table_queries:
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

        print('Droping tables if already exists...')
        drop_tables(cur, conn)

        print('Creating tables...')
        create_tables(cur, conn)

        conn.close()
        print('Tables were created.')
    except Exception as e:
        print(e)

    
if __name__ == "__main__":
    main()