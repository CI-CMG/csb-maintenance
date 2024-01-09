import json
import logging
import os
import time
import boto3
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))

TABLE = os.getenv('ORDERS_TABLE')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE)


def count_orders(num_days=30):
    # calc earliest request to consider
    start_time = (datetime.now(timezone.utc) - timedelta(days=num_days)).isoformat(timespec='seconds')

    response = table.scan(
        FilterExpression=Attr('last_update').gt(start_time) & Attr('SK').eq('ORDER') & Attr('status').eq('complete'),
        Select='COUNT'
    )

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception("failed to query table")

    return ({
        'count': response['Count'],
        'since': start_time
    })


def lambda_handler(event, context):
    response = count_orders()

    return response
