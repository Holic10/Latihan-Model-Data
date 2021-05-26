import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,analytical_queries,transform_queries


def load_staging_tables(cur, conn):
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def anal_queries(cur, conn):
    
    for query in analytical_queries:
        cur.execute(query)
        conn.commit()
        row = cur.fetchone()
        while row:
            print(row)
            row = cur.fetchone()

def trans_queries(cur, conn):
    
    for query in transform_queries:
        cur.execute(query)
        conn.commit()        
        
        
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Memuat table....')
   
    load_staging_tables(cur, conn)
    
    print('proses perubahan ')
   
    trans_queries(cur, conn)
    print('Masukkan tabel ke schema db')
   
    insert_tables(cur, conn)

    print('Menjalankan Queri :)')
   
    anal_queries(cur, conn)
    
    print('Finished! gonna close the database connection, BYEEEE :)')
    conn.close()


if __name__ == "__main__":
    main()