"""
construct and submit an Athena query using the provided parameters. Uses DDB
table to track queries.
Generally triggered by Step Function task
"""

import json
import boto3
import time
import os
import logging


logger = logging.getLogger()
logger.setLevel(os.getenv("LOGLEVEL", "WARNING"))

DATABASE = os.getenv('ATHENA_DATABASE')
ATHENA_RESULTS_BUCKET = f's3://{os.getenv("ATHENA_RESULTS_BUCKET_NAME")}/'
QUERY = f"select h3_hires, count(*) from {DATABASE}.{os.getenv('ATHENA_TABLE')} group by 1 order by 1"

dynamodb = boto3.resource('dynamodb')
athena = boto3.client('athena')
queries_table = dynamodb.Table(os.getenv('ATHENA_QUERIES_TABLE'))


def insert_query_id(query_id, label='CSB_H3_COUNT'):
    # expire records 24 hours after creation
    ttl = int(time.time()) + (24 * 60 * 60)
    attributes = {
        'id': query_id,
        'label': label,
        'ttl': ttl
    }
    response = queries_table.put_item(
        Item=attributes
    )
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception('insert into database failed')
    return attributes


def lambda_handler(event, context):
    response = athena.start_query_execution(
        QueryString=QUERY,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': ATHENA_RESULTS_BUCKET
        }
    )
    # store query reference to allow separate function to pickup results asynchronously
    query_execution_id = response['QueryExecutionId']
    insert_query_id(query_id=query_execution_id)

    return response
