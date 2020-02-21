import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""Drop, if exist, tables from SparkifyDB using DROP queries.
Parameterize drop_tables function with 2 arguments:
1. cur : cursor object.
2. conn : connection object.
"""
    
def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("An exception occured while executing: " + query)
            print(e)

    print("Command(s) completed successfully!")

"""Create new tables using CREATE queries.
Parameterize drop_tables function with 2 arguments:
1. cur : cursor object.
2. conn : connection object.
"""
    
    
def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("An exception occured while executing: " + query)
            print(e)
    print("Command(s) completed successfully!")
    
    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to REDSHIFT...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('REDSHIFT connected successfully!')
    cur = conn.cursor()

    print('Dropping  tables...')
    drop_tables(cur, conn)
    
    print('Creating tables...')
    create_tables(cur, conn)

    conn.close()
    print('Datawarehouse created successfully!')

if __name__ == "__main__":
    main()


