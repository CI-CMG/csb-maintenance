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


# output produced by parallel task is formatted a bit weird so separate into more
# usable data structures

def upload_data_to_s3(bucket, obj_key, data):
    with io.BytesIO() as f:
        for line in data:
            f.write(bytes(line, 'utf-8'))
        f.seek(0)
        bucket.upload_fileobj(f, obj_key)


def lambda_handler(event, context):
    print(event)

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
                'MinDate': i['MinDateTime'].split()[0],
                'MaxDate': i['MaxDateTime'].split()[0],
                # strip out header and truncate DateTimeString to yyyy-mm-dd
                'MonthlyCounts': [{'Month': x['Month'].split()[0], 'Count': int(x['Count'])} for x in
                                  i['MonthlyCounts'][1:]],
                'PlatformCounts': [
                    {'Platform': x['Platform'], 'Count': int(x['Count']), 'FirstSubmission': x['FirstSubmission'],
                     'LastSubmission': x['LastSubmission']} for x in i['PlatformCounts'][1:]],
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