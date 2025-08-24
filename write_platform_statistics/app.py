import json
from datetime import date
import io
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))

DELIVERY_BUCKET_NAME = os.getenv('DELIVERY_BUCKET_NAME')

s3 = boto3.resource('s3')
# TODO might be better to read the Athena query results via Boto3 rather than with Step Function
# athena = boto3.client('athena')


def upload_data_to_s3(bucket, obj_key, data):
    with io.BytesIO() as f:
        for line in data:
            f.write(bytes(line, 'utf-8'))
        f.seek(0)
        bucket.upload_fileobj(f, obj_key)


def lambda_handler(event, context):
    provider_name = event['Provider']
    result = {'report_date': date.today().isoformat()}

    result = {
        'report_date': date.today().isoformat(),
        'platforms': event['Results'][1:]
    }
    output_bucket = s3.Bucket(DELIVERY_BUCKET_NAME)
    obj_key = f"{provider_name.replace(' ','_')}.json"
    url = f"https://{DELIVERY_BUCKET_NAME}.s3.amazonaws.com/{obj_key}"
    arn = f's3://{DELIVERY_BUCKET_NAME}/{obj_key}'

    upload_data_to_s3(output_bucket, obj_key, json.dumps(result))

    return {
        'output_url': url,
        'output_arn': arn
    }
