import sqlite3
import config
from datetime import datetime
from extract import extract_data
from transform import transform_data
# from load import load_data
from logger import logs
logger,run_id=logs('etl_pipeline')
from upload_to_s3 import upload_to_s3
from load_postgresql import load_postgres

def run_pipeline():
    connection=None
    try:
        connection=sqlite3.connect(config.db)
        cursor=connection.cursor()
        cursor.execute('create table if not exists pipeline_runs (run_id text primary key,start_time datetime,end_time datetime,status text,stage text,error_message text,records_extracted, records_transformed, records_loaded)')
        start_time=datetime.now().isoformat()
        logger.info('Pipeline Started')
        raw=extract_data()
        
        if raw is None:
            logger.error('Pipeline aborted at extract')
            cursor.execute('insert into pipeline_runs (run_id,start_time,end_time,status,stage,error_message,records_extracted,records_transformed, records_loaded)values(?,?,?,?,?,?,?,?,?)',(run_id,start_time,datetime.now().isoformat(),'failed','extract','no values extracted',0,None,None))
            return
        rows_extracted=len(raw)
        cleaned=transform_data(raw)
      
        if cleaned is None:
            logger.error('Pipeline aborted at transform')
            cursor.execute('insert into pipeline_runs (run_id,start_time,end_time,status,stage,error_message,records_extracted, records_transformed, records_loaded)values(?,?,?,?,?,?,?,?,?)',(run_id,start_time,datetime.now().isoformat(),'failed','transform','no values cleaned',rows_extracted,0,None))
            return
        rows_cleaned=len(cleaned)
        upload_to_s3(cleaned)


        rows_loaded = load_postgres(cleaned)
        logger.info('Pipeline Completed')
        cursor.execute('insert into pipeline_runs (run_id,start_time,end_time,status,stage,error_message,records_extracted, records_transformed, records_loaded)values(?,?,?,?,?,?,?,?,?)',(run_id,start_time,datetime.now().isoformat(),'success',None,None,rows_extracted,rows_cleaned,rows_loaded))
    finally:
        if connection:
            connection.commit()
            connection.close()

if __name__=='__main__':
    run_pipeline()




    