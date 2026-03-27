from extract import extract_data
from transform import transform_data
from load import load_data
from logger import logs
logger,run_id=logs('etl_pipeline')
import sqlite3
import config
from datetime import datetime

def run_pipeline():
    connection=sqlite3.connect(config.db)
    cursor=connection.cursor()
    cursor.execute('create table if not exists pipeline_runs (run_id text primary key,start_time datetime,end_time datetime,status text,stage text,error_message text)')
    start_time=datetime.now()
    logger.info('Pipeline Started')
    raw=extract_data()
    if raw is None:
        logger.error('Pipeline aborted at extract')
        cursor.execute('insert into pipeline_runs (run_id,start_time,end_time,status,stage,error_message)values(?,?,?,?,?,?)',(run_id,start_time,datetime.now(),'failed','extract','no values extracted'))
        connection.commit()
        connection.close()
        return
    cleaned=transform_data(raw)
    if cleaned is None:
        logger.error('Pipeline aborted at transform')
        cursor.execute('insert into pipeline_runs (run_id,start_time,end_time,status,stage,error_message)values(?,?,?,?,?,?)',(run_id,start_time,datetime.now(),'failed','transform','no values cleaned'))
        connection.commit()
        connection.close()
        return
    load_data(cleaned)
    
    logger.info('Pipeline Completed')
    cursor.execute('insert into pipeline_runs (run_id,start_time,end_time,status,stage,error_message)values(?,?,?,?,?,?)',(run_id,start_time,datetime.now(),'success',None,None))
    connection.commit()
    connection.close()

if __name__=='__main__':
    run_pipeline()




    