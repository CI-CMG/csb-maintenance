import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))

client = boto3.client('stepfunctions')
s3 = boto3.resource('s3')

partition_path = os.environ.get("PARTITION_PATH", "csb/parquet/csb_h3_part/")
bucket_name = os.environ.get("BUCKET_NAME", "dcdb-data")


def send_success(task_token, payload=None):
    response = client.send_task_success(
        taskToken=task_token,
        output=json.dumps(payload)
    )
    return response


def send_failure(task_token, error_code='', error_cause=''):
    response = client.send_task_failure(
        taskToken=task_token,
        error=error_code,
        cause=error_cause
    )
    return response


def delete_parquet_files(bucket, partition_path, partition_name):
    prefix = f"{partition_path}h3={partition_name}"
    objects_iterable = bucket.objects.filter(Prefix=prefix)
    objects_to_delete = [{'Key': o.key} for o in objects_iterable]

    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/bucket/objects.html
    if len(objects_to_delete) > 1000:
        raise Exception('cannot delete more than 1,000 objects at once')

    logger.info(f"{len(objects_to_delete)} files to delete for partition {partition_name}")

    if len(objects_to_delete) == 0:
        logger.warn('no objects matching filter')
        return False

    logger.info(f'deleting {len(objects_to_delete)} objects from bucket {bucket.name}')
    response = bucket.delete_objects(
        Delete={
            'Objects': objects_to_delete,
            'Quiet': True
        }
    )

    if 'ResponseMetadata' in response and 'HTTPStatusCode' in response['ResponseMetadata'] and \
            response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logger.debug('Success!')
        logger.debug(response)
        # logger.debug('Success! deleted:')
        # for i in response['Deleted']:
        #     logger.debug(i['Key'])
        return {True}
    else:
        logger.error(response)
        raise Exception('error in deleting files')


def lambda_handler(event, context):
    if 'taskToken' not in event or 'partition' not in event:
        raise Exception('invalid payload')

    task_token = event['taskToken']
    partition_name = event['partition']
    bucket = s3.Bucket(bucket_name)

    delete_parquet_files(bucket, partition_path, partition_name)

    payload = {
        'statusCode': 204,
        'message': f'objects for partition {partition_name} deleted from bucket'
    }
    send_success(task_token, payload)

# Sample Payload
# {
#   'partition': '8023fffffffffff',
#   'taskToken': 'AQB4AAAAKgAAAAMAA...'
# }