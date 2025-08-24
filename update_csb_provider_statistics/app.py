"""
assemble and write out JSON file used by webapp
"""
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

def upload_data_to_s3(bucket, obj_key, data):
    with io.BytesIO() as f:
        for line in data:
            f.write(bytes(line, 'utf-8'))
        f.seek(0)
        bucket.upload_fileobj(f, obj_key)


def get_platform_data(bucket, obj_key):
    obj = s3.Object(bucket, obj_key)
    file_content = obj.get()['Body'].read().decode('utf-8')
    platform_data = json.loads(file_content)
    return platform_data['platforms']


def lambda_handler(event, context):
    # print(event)

    # initialize results dictionary
    result = {'report_date': date.today().isoformat(), 'providers': []}

    # in theory, the incoming data file should have the same keys every time
    expected_keys = ['Provider', 'RecordCount', 'PlatformCount', 'MinDateTime', 'MaxDateTime', 'PlatformCounts',
                     'MonthlyCounts']

    # create a list of dicts, one per provider
    for i in event:
        result['providers'].append(
            {
                'Provider': i['Provider'],
                'TotalSoundings': int(i['RecordCount']),
                'PlatformCount': int(i['PlatformCount']),
                # MinDate, MaxDate deprecated
                'MinDate': i['MinDateTime'].split()[0],
                'MaxDate': i['MaxDateTime'].split()[0],
                'FirstCollection': i['FirstCollection'].split()[0],
                'LastCollection': i['LastCollection'].split()[0],
                'FirstSubmission': i['FirstSubmission'].split()[0],
                'LastSubmission': i['LastSubmission'].split()[0],
                # strip out header and truncate DateTimeString to yyyy-mm-dd
                'MonthlyCounts': [{'Month': x['Month'].split()[0], 'Count': int(x['Count'])} for x in
                                  i['MonthlyCounts'][1:]],
                'PlatformCounts': get_platform_data(DELIVERY_BUCKET_NAME, i['PlatformCounts'].split('/')[-1]),
                'ActiveProvider': i['ActiveProvider'].strip().lower() == "true"
            }
        )

    output_bucket = s3.Bucket(DELIVERY_BUCKET_NAME)
    obj_key = 'csb_provider_statistics.json'
    url = f"https://{DELIVERY_BUCKET_NAME}.s3.amazonaws.com/{obj_key}"
    upload_data_to_s3(output_bucket, obj_key, json.dumps(result))
    return {
        'statusCode': 200,
        'body': json.dumps({'url': url})
    }