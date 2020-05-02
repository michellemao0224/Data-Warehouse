import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables if the table exists with same name in Redshift

    Args:
        param1: Connection cursor.
        param2: Connect AWS Redshift database with host name, database name, database credential and port.

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create staging tables, fact table and dimension tables in Redshift

    Args:
        param1: Connection cursor.
        param2: Connect AWS Redshift database with host name, database name, database credential and port.
        
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('Connecting to AWS Redshift...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Success: Connected to AWS Redshift')
    
    drop_tables(cur, conn)
    print('Success: Dropped exisiting tables')
    create_tables(cur, conn)
    print('Success: Created new tables')

    conn.close()
    print('Connection closed')

if __name__ == "__main__":
    main()