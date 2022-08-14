from airflow import DAG
import os 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from local_to_postgres import ingest_csv
from dotenv import load_dotenv 
load_dotenv()

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

load_initial_data_dag = DAG(
    'load_data',
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval = None,
)

with load_initial_data_dag:


    t4 = PythonOperator(
                        task_id='load_netflix',
                        python_callable=ingest_csv,
                        dag = load_initial_data_dag,
                        op_kwargs=dict(
                            user=PG_USER,
                            password=PG_PASSWORD,
                            host=PG_HOST,
                            port=PG_PORT,
                            table='netflix',
                            csv_path='/Users/Salman/Desktop/Github/Streamline/Data/netflix_titles.csv',
            
                        ),
                        )
    t4