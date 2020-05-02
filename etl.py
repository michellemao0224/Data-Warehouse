import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load staging tables from S3 bucket to Redswift cluster

    Args:
        param1: Connection cursor.
        param2: Connect AWS Redshift database with host name, database name, database credential and port.

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into Reshift fact table and dimention tables.

    Args:
        param1: Connection cursor.
        param2: Connect AWS Redshift database with host name, database name, database credential and port.

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to AWS Redshift...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Success: Connected to AWS Redshift')
    
    print('Loading staging tables from S3 to Redshift...')
    load_staging_tables(cur, conn)
    print('Success: Loaded staging tables')
    print('Inserting data into Redshift fact and dimension tables...')
    insert_tables(cur, conn)
    print('Success: Inserted data into Redshift')

    conn.close()
    print('Connection closed')

if __name__ == "__main__":
    main()