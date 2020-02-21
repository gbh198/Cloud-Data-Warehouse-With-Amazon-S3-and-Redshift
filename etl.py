import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


# Loading data from S3 to staging tables

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Excecuting query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('{} executed successfully.'.format(query))
        
""" Loading data from staging tables to 
tables in star schema (fact table and dimension ones)
"""

def insert_tables(cur, conn):
    print("...Loading data from staging tables into star schema...")
    for query in insert_table_queries:
        print('Excecuting query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('{} executed successfully.'.format(query))

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
  
    print('Connecting to REDSHIFT')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connection to REDSHIFT has been established.')
    cur = conn.cursor()
    
    print('Loading data from S3 to staging tables...')
    load_staging_tables(cur, conn)
    
    print('..Starting ETL processes....')
    insert_tables(cur, conn)

    conn.close()
    print('ETL process completed')


if __name__ == "__main__":
    main()  