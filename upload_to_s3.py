import boto3
import io
import csv 
import config 
from datetime import datetime
import logging
logger = logging.getLogger('etl_pipeline')


def upload_to_s3(data):
    timestamp=datetime.now().strftime('%Y%m%d-%H:%M:%S')
    filename = f'posts_{timestamp}.csv'

    buffer=io.StringIO()
    writer=csv.DictWriter(buffer,fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    csv_content=buffer.getvalue()

    s3 = boto3.client('s3',
    aws_access_key_id=config.access_key,
    aws_secret_access_key=config.secret_key,
    region_name=config.region
    )
    s3.put_object(
    Bucket=config.bucket_name,
    Key=filename,
    Body=csv_content
    )
    logger.info(f'CSV file written and uploaded to s3')