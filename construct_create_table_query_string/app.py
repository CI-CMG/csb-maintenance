import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))

def lambda_handler(event, context):
    """
    work around problem with Step Function's intrinsic function States. Format
    and construct SQL statement to create CSB table
    """

    DATABASE = event['ATHENA_DATABASE']
    CSV_TABLE = event['CSV_TABLE']
    PARQUET_TABLE = event['PARQUET_TABLE']
    S3_BUCKET = event['S3_BUCKET']

    # verify all expected env variables found
    if not (DATABASE and PARQUET_TABLE and S3_BUCKET and CSV_TABLE):
        logger.error("missing required environment variable")
        return

    result = {}
    result['globals'] = event.copy()

    result['QUERY_STRING'] = (
        f'CREATE TABLE {PARQUET_TABLE} '
        f'WITH ('
        f"format = 'Parquet', "
        f"parquet_compression = 'SNAPPY', "
        f"external_location = 's3://{S3_BUCKET}/csb/parquet/{PARQUET_TABLE}/' "
        f') '
        f'AS SELECT from_iso8601_date(entry_date) "entry_date", file_uuid, lon, lat, depth, cast(from_iso8601_timestamp(time) as timestamp) "time", platform_name, provider, unique_id, h3_hires, h3 FROM {DATABASE}.{CSV_TABLE}'
    )
    return result
