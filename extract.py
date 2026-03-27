import requests
import time
import logging
logger = logging.getLogger('etl_pipeline')
import config


def extract_data():
    logger.info('Extraction Started')
    retries=config.retry
    for retry in range(1,retries+1):
        try:
            response=requests.get(config.api)
            data=response.json()
            logger.info(f"Extracted {len(data)} records successfully")
            return data
        except Exception as e:
            if retry==retries:
                logger.error('retry limit reached')
                return None
            else:
                logger.warning(f'retrying {retry+1} in 10 seconds')
                time.sleep(10)
            
