import os 
from time import time 
import pandas as pd 
from sqlalchemy import create_engine
import psycopg2

def ingest_csv(user, password, host, port, table, csv_path, execution_date):
   
    db ='gcpdw'
    conn = psycopg2.connect(
    database=db, user=user, password=password, host=host, port= port
)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = {db}")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute('CREATE DATABASE {db}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    print("Connection Successful")
    cursor.execute("DROP TABLE IF EXISTS {table}")

    #Creating table as per requirement
    sql ='''CREATE TABLE {table}(
   show_id varchar,
   type varchar,
   title varchar,
   director varchar,
   cast varchar,
   country varchar,
   date_added datetime,
   release_year int,
   rating,duration varchar,
   listed_in varchar,
   description varchar
    )'''
    cursor.execute(sql)
    t_start = time()

    df_chunk = pd.read_csv(csv_path, iterator=True, chunksize=10000)

    df = next(df_chunk)


    #df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table, con=conn, if_exists='replace')

    df.to_sql(name=table, con=conn, if_exists='append')

    t_end = time()
    print('inserted the first chunk, took %.3f second' % (t_end - t_start))

    while True: 
        t_start = time()

        try:
            df = next(df_chunk)
        except StopIteration:
            print("completed")
            break

        #df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        #df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table, con=conn, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))




