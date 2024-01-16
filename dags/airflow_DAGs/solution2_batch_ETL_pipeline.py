from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from requests.auth import HTTPBasicAuth
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from scripts.solution2_extract import extract
from scripts.solution2_transform import transform
from scripts.solution2_load import load_to_s3
from scripts.solution2_read_from_s3 import read_from_s3
from scripts.solution2_write_to_data_warehouse import push_to_redshift

base_url = 'https://xecdapi.xe.com'
auth = HTTPBasicAuth('autocheck648265792', '5q1p8fjeeb8rapa62hcab49i2g')
relevant_isos = 'EGP, GHS, KES, MAD, NGN, UGX, XOF'
params = {'from':'USD', 'to':relevant_isos, 'obsolete':'false', 'inverse':'true'}
AWS_S3_BUCKET = 'xe-exchange-rates-data'
key = 'exchange_rates_data/daily_USD_to_local_rates.csv'

# Specify redshift connection credentials
url = URL.create(
    drivername='redshift+redshift_connector', 
    host='personal-redshift-cluster-1.cuf8ata0lkii.us-east-1.redshift.amazonaws.com', 
    port='5439',
    database='dev', 
    username='akawi', 
    password='AkawiCourage85' 
    )
conn_ = create_engine(url)
engine = conn_.connect()
print('Engine connected successfully!')

default_args={
    'owner':'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Instantiate the DAG with an ID and assign to a variable
ETL_dag = DAG(dag_id='Exchange_Rates_ETL_Pipeline', 
                default_args=default_args,
                description='ETL Pipeline to pull from xecd API, Transform the data and write to s3',
                schedule="0 1,23 * * *",
                start_date=days_ago(1),
                catchup=False,
                tags=['ETL', 'xecd', 'Autocheck'],
    )


task_1 = PythonOperator( 
    task_id='Extract_from_API', 
    python_callable=extract, 
    op_kwargs={'base_url': base_url, 'auth':auth, 'params':params},
    dag=ETL_dag,
)

task_2 = PythonOperator(
    task_id='Transform',
    python_callable = transform,
    provide_context=True,
    op_kwargs={'response': "{{ ti.xcom_pull(task_ids='Extract_from_API') }}"},
    dag=ETL_dag,
)

task_3 = PythonOperator(
    task_id='Load_to_s3',
    python_callable = load_to_s3,
    provide_context=True,
    op_kwargs={'df_new': "{{ ti.xcom_pull(task_ids='Transform') }}", 
                'bucket_name':AWS_S3_BUCKET, 'key':key},
    dag=ETL_dag,
)

task_4 = PythonOperator(
    task_id='Write_data_to_redshift_warehouse',
    python_callable = push_to_redshift,
    op_kwargs={'conn':engine, 'df':read_from_s3(bucket_name=AWS_S3_BUCKET, key=key), 
                'table_name':'usd_daily_exchange_rates', 'insertion_type':'replace', },
    dag=ETL_dag,
)

task_1 >> task_2 >> task_3 >> task_4
