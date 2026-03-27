from extract import extract_data
from transform import transform_data
from load import load_data
from logger import logs
logger,run_id=logs('etl_pipeline')

def run_pipeline():
    logger.info('Pipeline Started')
    raw=extract_data()
    if raw is None:
        logger.error('Pipeline aborted at extract')
        return
    cleaned=transform_data(raw)
    if cleaned is None:
        logger.error('Pipeline aborted at transform')
        return
    load_data(cleaned)
    
    logger.info('Pipeline Completed')

if __name__=='__main__':
    run_pipeline()




    