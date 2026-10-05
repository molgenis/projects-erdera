"""
Fetch EGA data using the egaClient
This script logs into the EGA API and fetches the metadata belonging to an EGA dataset (with provisional ID)

## For deployment

1. Copy the contents of this script into the script editor UI
2. Copy the following requirements into the 'dependencies' field
3. Save and run

### Dependencies

pandas
python-dotenv
molgenis-emx2-pyclient
"""

import logging
import sys
import time
from datetime import datetime
from os import environ

import pandas as pd
from dotenv import load_dotenv
from molgenis_emx2_pyclient import Client

from erdera.clients.egaClient import EGASubmissionsClient

load_dotenv()

# set environment variables
MOLGENIS_HOST = 'http://localhost:8080/'
MOLGENIS_TOKEN = environ['MOLGENIS_TOKEN']
SCHEMA_JOBS = 'Jobs'
SCHEMA_EGA_SOURCE = 'Staging Area Ega'
OUTPUT_FILE = environ["OUTPUT_FILE"]

if environ.get('MOLGENIS_HOST'):
    MOLGENIS_HOST = environ['MOLGENIS_HOST']

# write logs to an output file instead of to the screen
logging.basicConfig(level='INFO', filename=OUTPUT_FILE)
# set level of the logger of the requests library
logging.getLogger("requests").setLevel(logging.WARNING)
# set level of the logger of the urllib3 library
logging.getLogger("urllib3").setLevel(logging.WARNING)
# make sure warnings from the standard warning module are written to the log file
logging.captureWarnings(True)
# set logger
log = logging.getLogger("Mapping EGA to staging area")

# if the script is deployed on the server, retrieve the dataset to migrate, the username and password from the command line arguments
if len(sys.argv) > 1:
    ACCESSION_ID = sys.argv[1]
    if not ACCESSION_ID.startswith('EGAD'):
        log.error('First argument is not the EGA dataset accession ID. The ID is formatted as EGAD<number>')
        sys.exit('Error: wrong argument.')

    log.info('Received arg: Dataset accession ID: %s',
             ACCESSION_ID)
else:
    log.error('No argument was given. Please provide an EGA dataset accession ID.')
    sys.exit('Error: no argument was provided.')
    
if environ.get('PROVISIONAL_ID'):
    ACCESSION_ID = environ['PROVISIONAL_ID']

def prepare_run_metadata():
    """Prepare run metadata object"""
    return {
        'id': f'{datetime.now().strftime("%Y-%m-%d")}-run-{datetime.now().strftime("%H%M")}',  # noqa: DTZ005
        'date of run': datetime.now().strftime("%Y-%m-%d"),  # noqa: DTZ005
        'ok': False,
        'total number of datasets': 0,
        'number of new datasets': 0,
        'number of updated datasets': 0,
        'total number of analyses': 0,
        'number of new analyses': 0,
        'number of updated analyses': 0,
        'total number of analysis_samples': 0,
        'number of new analysis_samples': 0,
        'number of updated analysis_samples': 0,
        'total number of experiments': 0,
        'number of new experiments': 0,
        'number of updated experiments': 0,
        'total number of run_samples': 0,
        'number of new run_samples': 0,
        'number of updated run_samples': 0,
        'total number of runs': 0,
        'number of new runs': 0,
        'number of updated runs': 0,
        'total number of sample_files': 0,
        'number of new sample_files': 0,
        'number of updated sample_files': 0,
        'total number of samples': 0,
        'number of new samples': 0,
        'number of updated samples': 0,
        'total number of studies': 0,
        'number of new studies': 0,
        'number of updated studies': 0,
        'total number of study_analysis_samples': 0,
        'number of new study_analysis_samples': 0,
        'number of updated study_analysis_samples': 0,
        'total number of study_experiment_run_samples': 0,
        'number of new study_experiment_run_samples': 0,
        'number of updated study_experiment_run_samples': 0,
        'total number of files': 0,
        'number of new files': 0,
        'number of updated files': 0,
        'number of errors': 0,
    }

if __name__ == "__main__":
    
    # OPTION 1: retrieving all metadata (you need access with an account)
    endpoints = ['studies', 'samples', 'analyses', 'files', 'mappings/sample_file', 'mappings/analysis_sample', \
                'mappings/study_analysis_sample', 'experiments', 'runs', 'mappings/run_sample', 'mappings/study_experiment_run_sample']
    
    # OPTION 2: retrieving just the file metadata (you don't need access)
    endpoints = ['files']

    # initialise a client
    client = EGASubmissionsClient()

    # set provisional ID
    provisional_id = ACCESSION_ID

    api_run_errors = []
    api_run_meta = prepare_run_metadata()
    ega_output_data = {}
    for endpoint in endpoints:
        try:
            log.info(f'Fetching data from {endpoint}')
            endpoint_clean = endpoint.replace('mappings/', '')
            include_headers = True
            if endpoint == 'files':
                include_headers = False
            response = client.get_endpoint_dataset(provisional_id=provisional_id, endpoint=endpoint, include_headers=include_headers)
            dataset = pd.DataFrame(response.get('data'))
            dataset['dataset_accession_id'] = provisional_id
            dataset['added by job'] = api_run_meta['id']   
            ega_output_data[endpoint_clean] = dataset
            api_run_meta[f'total number of {endpoint_clean}'] = dataset.shape[0]
        
            if response.get('errors'):
                api_run_errors.extend(response.errors)
                api_run_meta['number of errors'] += response.get('errorCount')
            time.sleep(0.4)
        except Exception as error:  # noqa: BLE001
            log.error('Error in processing endpoint %s %s',
                      endpoint,
                      error)

    # fetching the information from the datasets endpoint
    log.info('Fetching data from datasets')
    response = client.get_endpoint_dataset(provisional_id=provisional_id, include_headers=False)
    dataset = pd.DataFrame([response.get('data')])
    dataset['added by job'] = api_run_meta['id']
    ega_output_data['dataset'] = dataset
    if response.get('errors'):
        api_run_errors.extend(response.errors)
        api_run_meta['number of errors'] += response.get('errorCount')
    api_run_meta['total number of datasets'] = dataset.shape[0]
        
    if api_run_errors:
        api_run_errors = pd.DataFrame(api_run_errors)
        api_run_errors['job'] = api_run_meta['id']

    if api_run_meta['number of errors'] == 0:
        log.info('No errors detected')
        api_run_meta['ok'] = True

    api_run_meta_df = pd.DataFrame([api_run_meta])
    api_run_meta_df['ok'] = api_run_meta_df['ok'].replace({True:'true', False: 'false'})

    # upload the data
    with Client(url=MOLGENIS_HOST,
                schema= SCHEMA_JOBS,
                token=MOLGENIS_TOKEN) as molgenis:

        molgenis.save_table(table='Jobs Ega Api', data=api_run_meta_df)

        if api_run_errors:
            molgenis.save_table(
                table='Job errors', data=api_run_errors)
    
    for key in ega_output_data:  # noqa: PLC0206
        # import into the staging area 
        with Client(url=MOLGENIS_HOST,
                    schema= SCHEMA_EGA_SOURCE,
                    token=MOLGENIS_TOKEN) as molgenis:

            molgenis.save_table(table=key, data=ega_output_data[key])
    
