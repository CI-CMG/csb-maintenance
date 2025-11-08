import json
import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))

source_table = os.environ.get("CSV_TABLE", "dcdb.csb_h3")


def lambda_handler(event, context):
    for i in ['partitions', 'tablename']:
        if i not in event:
            raise Exception('invalid payload')

    target_table = event['tablename']  # e.g. 'dcdb.csb_h3_part'
    partitions = event['partitions']  # e.g. ['8049fffffffffff']

    part_str = ','.join([f"'{i}'" for i in partitions])
    sql = f"""INSERT INTO {target_table}
        SELECT 
            from_iso8601_date(entry_date) "entry_date", 
            file_uuid, 
            lon, 
            lat, 
            depth, 
            cast(from_iso8601_timestamp(time) as timestamp) "time", 
            platform_name, 
            provider, 
            unique_id, 
            h3_hires, h3 
        FROM {source_table}
        WHERE h3 in ({part_str})"""

    logging.info(f"sql: {sql}")

    return {
        "query_string": sql
    }
