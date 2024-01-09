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
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))

DATABASE = os.getenv('ATHENA_DATABASE')
TABLE = os.getenv('ATHENA_TABLE')
OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET')
INPUT_BUCKET = os.getenv('ATHENA_INPUT_BUCKET')
QUERIES_TABLE = os.getenv('ATHENA_QUERIES_TABLE')
ATHENA_OUTPUT_LOCATION = f"s3://{OUTPUT_BUCKET}/"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE)
athena = boto3.client('athena')
queries_table = dynamodb.Table(QUERIES_TABLE)


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
    # print(event)

    # Database specified in Execution Context below.
    query = f"select h3_hires, count(*) from {DATABASE}.{TABLE} group by 1 order by 1"

    # Execution
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': ATHENA_OUTPUT_LOCATION
        }
    )

    # get query execution id
    query_execution_id = response['QueryExecutionId']
    # print(response)
    # query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    # query_execution_status = query_status['QueryExecution']['Status']['State']
    insert_query_id(query_id=query_execution_id)

    return response
