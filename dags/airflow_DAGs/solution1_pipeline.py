from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os
from sqlalchemy import create_engine
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from scripts.solution1_script import main

query_file_path='/opt/airflow/sql_scripts'

postgres_hostname='postgres'
postgres_username='airflow'
postgres_password='airflow'
postgres_databasename='airflow'
postgres_port='5432'

engine = create_engine(f'postgresql://{postgres_username}:{postgres_password}@{postgres_hostname}:{postgres_port}/{postgres_databasename}')
print("Trying to connect to postgres database")
engine.connect()
print("Successfully connected to postgres")

data_path = '/opt/airflow/data/'
script_path = '/opt/airflow/sql_scripts/'

default_args={
    'owner':'Akawi',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=3)
}

# Instantiate the DAG with an ID and assign to a variable
ETL_dag = DAG(dag_id='Solution_One_Pipeline', 
                default_args=default_args,
                description='ETL Pipeline to write local sample assessment files to postgres database and generate transformed data',
                schedule="0 0 * * *",
                start_date=days_ago(1),
                catchup=False,
                tags=['ETL', 'BATCH', 'Autocheck', 'Assessment'],
    )

task = PythonOperator(
    task_id='Solution_1',
    python_callable=main,
    op_kwargs={
        "engine":engine,
        "data_path":data_path,
        "script_path":script_path,
        },
    dag=ETL_dag,
)

task