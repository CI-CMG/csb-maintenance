import json
from datetime import date
import io
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "WARNING"))

OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET')
s3 = boto3.resource('s3')


# output produced by parallel task is formatted a bit weird so separate into more
# usable data structures

def upload_data_to_s3(bucket, obj_key, data):
    with io.BytesIO() as f:
        for line in data:
            f.write(bytes(line, 'utf-8'))
        f.seek(0)
        bucket.upload_fileobj(f, obj_key)


# incoming data just has one record for each active provider in given year.
# need to convert to one record for every provider for every year and replace
# annual count with running total per provider as well as percent of archive
def format_annual_counts(stats):
    # reshape athena output into list of lists in format [date, provider, count]
    raw_data = [[i['Data'][0]['VarCharValue'], i['Data'][1]['VarCharValue'], i['Data'][2]['VarCharValue']] for i in
                stats]

    # TODO convert raw data into format suitable for stacked bar chart
    # transform the year from string (e.g. '2017-01-01') to integer (e.g. 2017)
    data = [[int(i[0][0:4]), i[1], int(i[2])] for i in raw_data]  # loop more readable than list comprehension?

    # get unique years
    all_years = list(set([x[0] for x in data]))
    # get unique providers
    all_providers = list(set([x[1] for x in data]))

    # initialize running totals
    totals = {}
    for yr in all_years:
        totals[yr] = {}
        for provider in all_providers:
            totals[yr][provider] = 0

    # add running total for each provider to incoming data. expects data to be sorted by year
    prev_year = None
    for row in data:
        year = row[0]
        name = row[1]
        count = row[2]

        # populate the running totals
        if prev_year != year:
            # print(f'new year: {year}')
            if prev_year in totals:
                # print(f'carrying values forward from {prev_year} to {year}')
                for provider in all_providers:
                    totals[year][provider] = totals[prev_year][provider]
            prev_year = year
        # print(f'adding values to {year} ')
        totals[year][name] = totals[year][name] + count

        # add the running total to the record
        row.append(totals[year][name])

    # calculate and add yearly totals to running totals
    for year in all_years:
        totals[year]['YEARLY_TOTAL'] = sum(totals[year].values())

    # add yearly percentages for each provider to incoming data
    for row in data:
        year = row[0]
        total = row[3]

        pct_of_archive = round((total / totals[year]['YEARLY_TOTAL']) * 100)
        row.append(pct_of_archive)

    # write out final result, e.g
    # {"year":2017,"provider":"Orange Force Marine","total":0,"percentage_of_archive":0}
    result = []
    for year in all_years:
        for provider in all_providers:
            total = totals[year][provider]
            pct_of_archive = round((total / totals[year]['YEARLY_TOTAL']) * 100)
            result.append({'year': year, 'provider': provider, 'total': total, 'percentage_of_archive': pct_of_archive})

    return result


def format_counts(stats):
    return dict(zip(
        [i['Data'][0]['VarCharValue'] for i in stats],
        [i['Data'][1]['VarCharValue'] for i in stats]
    ))


def lambda_handler(event, context):
    # print(event)
    result = {'report_date': date.today().isoformat()}
    for i in event:
        # if 'counts_by_month' in i:
        #     result['counts_by_month'] = format_counts(i['counts_by_month'])
        if 'counts_by_year' in i:
            result['counts_by_year'] = format_annual_counts(i['counts_by_year'])
        if 'record_count' in i:
            result['record_count'] = i['record_count']
        if 'min_entry_date' in i:
            result['min_entry_date'] = i['min_entry_date']
        if 'max_entry_date' in i:
            result['max_entry_date'] = i['max_entry_date']
        if 'counts_by_provider' in i:
            result['counts_by_provider'] = format_counts(i['counts_by_provider'])
        # if 'counts_by_h3' in i:
        #     result['counts_by_h3'] = format_counts(i['counts_by_h3'])
        if 'order_count' in i:
            result['order_count'] = i['order_count']
        if 'fy_counts' in i:
            result['fy_counts'] = i['fy_counts']

    bucket_name = OUTPUT_BUCKET.strip('/').split('/')[-1]
    output_bucket = s3.Bucket(bucket_name)
    obj_key = 'csb_statistics.json'
    url = f"https://{bucket_name}.s3.amazonaws.com/{obj_key}"
    upload_data_to_s3(output_bucket, obj_key, json.dumps(result))
    return {
        'statusCode': 200,
        'body': json.dumps({'url': url})
    }
