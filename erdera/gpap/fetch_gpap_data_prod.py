"""
Fetch GPAP data using the GpapClient
This script logs into the GPAP API and fetches participants and experiments
"""

import os
import math
import logging
import json
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from molgenis_emx2_pyclient import Client
from erdera.clients.gpap.gpap_client_prod import GpapClient
import erdera.clients.gpap.gpap_client_types as types
from erdera.utils.index import date_now, date_today
load_dotenv()

logging.basicConfig(level=logging.INFO)
logging.captureWarnings(True)
log = logging.getLogger("GPAP API")


def get_all_metadata(
    client, meta_type: types.MetadataTypes, total_pages: int
) -> types.JobOutput:
    """Retrieve all participant or experiment metadata in batches

    :param client: a GPAP API instance

    :param meta_type: API metadata type to retrieve, 'participants' or 'experiments'
    :type meta_type: str

    :param total_pages: total number of pages to retrieve
    :type total_pages: int

    :returns: object containing successfully retrieved data, statuses, and error count
    :rtype: AllMetadataOutput
    """
    output: types.JobOutput = {
        'data': [],
        'errors': [],
        'errorCount': 0,
    }

    for page in range(0, total_pages, 1):
        page_num = page+1
        try:
            log.info('Retrieving data from page %s', page_num)
            page_data_rows: list[dict] = []

            if meta_type == 'participants':
                page_data = client.get_participants(page=page_num)
                page_data_rows = page_data['rows']
            elif meta_type == 'experiments':
                page_data = client.get_experiments(page=page_num)
                page_data_rows = page_data['items']
            else:
                log.error("Metadata type %s is not recognised", meta_type)
                break

            for page_row in page_data_rows:
                if isinstance(page_row, dict):
                    output['data'].append(page_row)

        except requests.exceptions.HTTPError as err:
            page_error = {
                'type': f'HTTP: {err.response.status_code}',
                'message': f"Unable to process page {page_num} {err} (HTTP: {err.response.status_code})"
            }

            output['errorCount'] += 1
            output['errors'].append(page_error)
            log.warning(page_error['message'])

        except KeyError as err:
            page_error = {'type': 'KeyError', 'message': ''}
            if err == 'rows':
                page_error['message'] = f"No data found on page {page_num} ('rows' not found)"
            else:
                page_error['message'] = f"Key '{err}' not found"

            output['errorCount'] += 1
            output['errors'].append(page_error)
            log.warning(page_error['message'])

        time.sleep(0.200)

    return output


def prepare_run_metadata() -> types.JobsGpapApi:
    """Prepare run metadata object
    :returns: metadata object for importing into the staging area
    :rtype: types.JobsGpapApi
    """
    return {
        'id': f"{date_today()}-run-{date_now()}",
        'date of run': date_today(),
        'ok': False,
        'total number of participants': 0,
        'number of new participants': 0,
        'number of updated participants': 0,
        'total number of experiments': 0,
        'number of new experiments': 0,
        'number of updated experiments': 0,
        'number of errors': 0
    }


if __name__ == "__main__":

    # load api fields
    with open('ERDERA/scripts/clients/gpap_prod_api_fields.json', mode='r', encoding='utf-8') as file:
        fields = json.load(file)
        file.close()

    # init client
    gpap = GpapClient(
        api_url=os.getenv("GPAP_PROD_API_URL"),
        token=os.getenv('GPAP_API_TOKEN')
    )
    gpap.api_page_size = 100
    gpap.fields = fields

    # retrieve all participants
    participants: types.ParticipantsResponse = gpap.get_participants()

    # temporary workaround: calculate page sizes
    if (participants['total'] % gpap.api_page_size) != 0:
        total_api_pages = math.ceil(participants['total'] / gpap.api_page_size)
    else:
        total_api_pages = participants['total'] / gpap.api_page_size

    log.info("Fetching participant metadata (%s records over %s pages)",
             participants['total'], total_api_pages)

    all_participants = get_all_metadata(
        client=gpap,
        meta_type='participants',
        total_pages=total_api_pages
    )

    # retrieve all experiments
    experiments: types.ExperimentsResponse = gpap.get_experiments()
    log.info("Fetching experiment metadata (%s records over %s pages)",
             experiments['_meta']['total_items'],
             experiments['_meta']['total_pages'])

    all_experiments = get_all_metadata(
        client=gpap,
        meta_type='experiments',
        total_pages=experiments['_meta']['total_pages']
    )

    # prepare exports and job metadata
    api_run_meta = prepare_run_metadata()

    participants_df = pd.DataFrame(all_participants['data'])
    experiments_df = pd.DataFrame(all_experiments['data'])

    participants_df['added by job'] = api_run_meta['id']
    experiments_df['added by job'] = api_run_meta['id']

    api_run_meta['total number of participants'] = participants_df.shape[0]
    api_run_meta['total number of experiments'] = experiments_df.shape[0]
    api_run_meta['number of errors'] = len(all_participants['errors']) + \
        len(all_experiments['errors'])

    if api_run_meta['number of errors'] == 0:
        log.info('No errors detected')
        api_run_meta['ok'] = True

    # prepare error messages
    api_run_errors = []
    participants_run_errors = pd.DataFrame(all_participants['errors'])
    experiments_run_errors = pd.DataFrame(all_experiments['errors'])

    if len(participants_run_errors):
        participants_run_errors['type'] = 'GPAP API participants retrieval'
        api_run_errors.extend(participants_run_errors)

    if len(experiments_run_errors):
        experiments_run_errors['type'] = 'GPAP API Experiments retrieval'
        api_run_errors.extend(experiments_run_errors)

    if api_run_errors:
        api_run_errors = pd.DataFrame(api_run_errors)
        api_run_errors['job'] = api_run_meta['id']

    api_run_meta_df = pd.DataFrame([api_run_meta])
    api_run_meta_df['ok'] = api_run_meta_df['ok'].replace(
        {True: 'true', False: 'false'})

    # import datasets into staging area
    log.info("Importing data into the staging area")
    with Client(url=os.getenv('EMX2_HOST'),
                schema=os.getenv('EMX2_HOST_SCHEMA'),
                token=os.getenv('EMX2_HOST_TOKEN')) as molgenis:

        molgenis.save_schema(table='Jobs Gpap Api', data=api_run_meta_df)

        if api_run_errors:
            molgenis.save_schema(
                table='Job errors', data=api_run_errors)

        molgenis.save_schema(table="Participants", data=participants_df)
        molgenis.save_schema(table="Experiments", data=experiments_df)
