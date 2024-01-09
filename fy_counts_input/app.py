import json
from datetime import date
import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))


def lambda_handler(event, context):
    start_year = event['start_year'] if 'start_year' in event else 2017
    current_year = date.today().year + 1

    response = []

    for yr in range(start_year, current_year):
        response.append({
            'name': f'FY{str(yr)[2:]}',
            'start': f'{yr - 1}-10-01',
            'end': f'{yr}-09-30'
        })

    return {'years': response}
