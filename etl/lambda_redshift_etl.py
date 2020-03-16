import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, delete_table_queries

"""
    Lambda function for procesing sensor reading JSON files
    Populates fact and dimension tables in Redshift
"""

def delete_staging_tables(cur, conn):
    for query in delete_table_queries:
        print('running query: ' + query)
        cur.execute(query)
        conn.commit()

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('running query: ' + query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('running query: ' + query)
        cur.execute(query)
        conn.commit()

def main(event, context):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('connected to Redshift cluster')

    print('deleting from staging tables')
    delete_staging_tables(cur, conn)    
    
    print('loading staging tables')
    load_staging_tables(cur, conn)

    print('inserting data')
    insert_tables(cur, conn)

    conn.close()