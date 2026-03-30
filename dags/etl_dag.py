import datetime
import sys
sys.path.insert(0,'/home/tamiz/etl_pipeline')
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import extract_data
from transform import transform_data
from load import load_data
default_args = {
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1)
}

with DAG(
    dag_id='etl_pipeline',
    start_date=datetime.datetime(2026,3,30),
    schedule='@daily',
    catchup=False,
    default_args=default_args
    
) as dag:
   
    def extract_wrapper():
        raw=extract_data()
        return raw
    
    def transform_wrapper(**kwargs):
            ti = kwargs['ti']
            raw = ti.xcom_pull(task_ids='extract')
            cleaned = transform_data(raw)
            return cleaned
    
    def load_wrapper(**kwargs):
         ti=kwargs['ti']
         cleaned=ti.xcom_pull(task_ids='transform')
         load_data(cleaned)

    task_extract = PythonOperator(
    task_id='extract',
    python_callable=extract_wrapper
    )
    task_transform=PythonOperator(
         task_id='transform',
         python_callable=transform_wrapper
    )
    task_load=PythonOperator(
         task_id='load',
         python_callable=load_wrapper
    )
    task_extract >> task_transform >> task_load