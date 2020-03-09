import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    print('dropping tables')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    print('creating tables')
    for query in create_table_queries:
        print('running query: ' + query)
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('connected to Redshift cluster')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()