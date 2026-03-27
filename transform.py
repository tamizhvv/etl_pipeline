from datetime import datetime,timezone
from logger import logs
logger,run_id=logs('etl_pipeline')

def transform_data(data):
    logger.info('Transformation Started')
    clean_records=[]
    for d in data:
        if not d['title'].strip() or not d['body'].strip():
            continue
        d['title']=d['title'].strip().upper()
        d['body']=d['body'].strip()
        d['loaded_at']=datetime.now(timezone.utc).isoformat()
        clean_records.append(d)
    if len(clean_records) / len(data) < 0.8:
        logger.warning(f"Only {len(clean_records)} of {len(data)} records passed. Below 80% threshold. Aborting.")
        return None

    logger.info(f"Transform completed. {len(clean_records)} clean records from {len(data)} raw records.")
    return clean_records

