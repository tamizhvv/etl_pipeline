import datetime
import sys
sys.path.insert(0,'/home/tamiz/etl_pipeline')
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import extract_data
from transform import transform_data
from load import load_data
from upload_to_s3 import upload_to_s3,download_from_s3
from load_postgresql import load_postgres
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
   
   def extract_wrapper(**kwargs):
    ti = kwargs['ti']
    raw = extract_data()
    s3_key = upload_to_s3(raw)
    row_count = len(raw)
    ti.xcom_push(key='extract_meta', value={'s3_key': s3_key, 'row_count': row_count})
    
    def transform_wrapper(**kwargs):
            ti = kwargs['ti']
            meta = ti.xcom_pull(task_ids='extract', key='extract_meta')
            s3_key=meta['s3_key']
            raw=download_from_s3(s3_key)
            cleaned = transform_data(raw)
            s3_key_tr=upload_to_s3(cleaned)
            row_count=len(cleaned)
            ti.xcom_push(key='transform_meta', value={'s3_key': s3_key_tr, 'row_count': row_count})

    
    def load_wrapper(**kwargs):
        ti=kwargs['ti']
        meta=ti.xcom_pull(task_ids='transform',key='transform_mets')
        data=meta['s3_key']
        s3_key=download_from_s3(data)
        load_postgres
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