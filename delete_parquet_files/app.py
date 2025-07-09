import json
import logging
import os
import boto3
from datetime import datetime
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))


def delete_external_files(table_name, bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    # depends on convention of <BUCKET_NAME>/csb/parquet/<TABLE_NAME>
    print("table: " + table_name)
    print("bucket: " + bucket_name)
    # objects = bucket.objects.filter(Prefix = f'csb/parquet/{table_name}')
    objects = bucket.objects.filter(Prefix='csb/parquet/csb_parquet')

    print(objects)
    objects_to_delete = [{'Key': o.key} for o in objects]
    print(objects_to_delete)
    logger.debug('deleting files...')
    print(objects_to_delete)
    # if len(objects_to_delete):
    #     response = s3.meta.client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete, 'Quiet': False})

    logger.info(f'deleted {len(objects_to_delete)} files from bucket {bucket_name}\n')


def delete_parquet_files(prefix, bucket):
    objects_iterable = bucket.objects.filter(Prefix=prefix)
    objects_to_delete = [{'Key': o.key} for o in objects_iterable]

    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/bucket/objects.html
    if len(objects_to_delete) > 1000:
        raise Exception('cannot delete more than 1,000 objects at once')

    if len(objects_to_delete) == 0:
        logger.warn('no objects matching filter')
        return False

    print(f'deleting {len(objects_to_delete)} objects from bucket {bucket.name}')
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
    PARQUET_TABLE = event['PARQUET_TABLE']
    S3_BUCKET = event['S3_BUCKET']

    # verify all expected env variables found
    if not (S3_BUCKET and PARQUET_TABLE):
        logger.error("missing required parameters")
        return
    object_prefix = f'csb/parquet/{PARQUET_TABLE}'
    # object_prefix = f'test/parquet/{PARQUET_TABLE}'
    # delete_external_files(table_name=PARQUET_TABLE, bucket_name=S3_BUCKET)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET)
    result = delete_parquet_files(prefix=object_prefix, bucket=bucket)

    response = {}
    response['globals'] = event.copy()
    # print(response)
    return response
