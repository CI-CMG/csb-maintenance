"""
reads the Athena query results (CSV file) and writes a JSON formatted output to
bucket.
Triggered by EventBridge rule which is filtered to only provide Athena events
with a status of 'FAILED' or 'SUCCEEDED'. Reads from a DDB table to determine
whether the received Athena QueryExecutionId is from the H3 query of interest.
"""

import json
import logging
import boto3
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))

DATABASE = os.getenv('ATHENA_DATABASE')
TABLE = os.getenv('ATHENA_TABLE')
S3_OUTPUT = os.getenv('ATHENA_OUTPUT_BUCKET')

QUERIES_TABLE = os.getenv('ATHENA_QUERIES_TABLE')
OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET')
INPUT_BUCKET = os.getenv('INPUT_BUCKET')
QUERY_LABEL = os.getenv('QUERY_LABEL', default='CSB_H3_COUNT')

athena = boto3.client('athena')
dynamodb = boto3.resource('dynamodb')
s3 = boto3.resource('s3')
ddb_table = dynamodb.Table(QUERIES_TABLE)


def is_query_of_interest(queryid, label=QUERY_LABEL):
    response = ddb_table.get_item(
        Key={'id': queryid, 'label': label}
    )
    return 'Item' in response


def lambda_handler(event, context):
    queryExecutionId = event['detail']['queryExecutionId']

    # query DDB to see if this query is a h3 query
    query_of_interest = is_query_of_interest(queryExecutionId)

    if not query_of_interest:
        logger.debug(f'query id {queryExecutionId} is not of interest')
        return

    if event['detail']['currentState'] != 'SUCCEEDED':
        logger.warning(f"Failed query: {queryExecutionId}")
        return

    # convert the output CSV into JSON w/ standard name
    # WARNING: loads entire file into memory. may not scale for really large files
    logger.info(f'reading s3://{INPUT_BUCKET}/{queryExecutionId}.csv...')
    obj = s3.Object(INPUT_BUCKET, f'{queryExecutionId}.csv')
    lines = obj.get()['Body'].iter_lines()
    # skip first line (assumed to be header)
    next(lines)

    records = []
    for line in lines:
        # strip the quotes from CSV list
        key, value = [item.strip('"') for item in line.decode().split(',')]
        records.append({
            'h3': key,
            'count': int(value)
        })

    # format count with commas separating thousands
    logger.info(f'{"{:,}".format(len(records))} records processed')

    data_string = json.dumps({'counts_by_h3': records}, indent=2, default=str)
    s3_resource = boto3.resource('s3')
    s3_bucket = s3_resource.Bucket(name=OUTPUT_BUCKET)

    s3_bucket.put_object(
        Key='csb_counts_by_h3.json',
        Body=data_string
    )
    logger.info(f'output written to {OUTPUT_BUCKET}/csb_counts_by_h3.json')

    # print(event)

# {
#     'version': '0',
#     'id': 'ddd2dae3-39d2-7ebf-6066-38df6a7176bb',
#     'detail-type': 'Athena Query State Change',
#     'source': 'aws.athena',
#     'account': '282856304593',
#     'time': '2023-03-09T00:26:04Z',
#     'region': 'us-east-1',
#     'resources': [],
#     'detail': {
#         'currentState': 'SUCCEEDED',
#         'previousState': 'RUNNING',
#         'queryExecutionId': '2b1a5ee0-a30e-490a-b50e-d06a2c8ff1f5',
#         'sequenceNumber': '3',
#         'statementType': 'DML',
#         'versionId': '0',
#         'workgroupName': 'primary'
#     }
# }