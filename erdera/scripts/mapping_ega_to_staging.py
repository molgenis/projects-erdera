"""
Fetch EGA data using the egaClient
This script logs into the EGA API and fetches the metadata belonging to an EGA dataset
"""

import os
import logging
import pandas as pd
from dotenv import load_dotenv
from molgenis_emx2_pyclient import Client
from erdera.clients.egaClient import EGASubmissionsClient
from os import environ
from datetime import datetime
load_dotenv()
import time

logging.basicConfig(level=logging.INFO)
logging.captureWarnings(True)
log = logging.getLogger("GPAP API")

def prepare_run_metadata():
    """Prepare run metadata object"""
    return {
        'id': f"{datetime.now().strftime("%Y-%m-%d")}-run-{datetime.now().strftime("%H%M")}",
        'date of run': datetime.now().strftime("%Y-%m-%d"),
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

    # retrieve the data
    ega_output_data = {}
    endpoints = ['studies', 'samples', 'analyses', 'files', 'mappings/sample_file', 'mappings/analysis_sample', \
                 'mappings/study_analysis_sample', 'experiments', 'runs', 'mappings/run_sample', 'mappings/study_experiment_run_sample']
    
    client = EGASubmissionsClient()
    provisional_id = environ['PROVISIONAL_ID']

    api_run_errors = []
    api_run_meta = prepare_run_metadata()
    ega_output_data = {}
    for endpoint in endpoints:
        try:
            logging.info(f'Fetching data from {endpoint}')
            endpoint_clean = endpoint.replace('mappings/', '')
            response = client.get_endpoint_dataset(provisional_id=provisional_id, endpoint=endpoint)
            dataset = pd.DataFrame(response.get('data'))
            dataset['added by job'] = api_run_meta['id']   
            ega_output_data[endpoint_clean] = dataset
            api_run_meta[f'total number of {endpoint_clean}'] = dataset.shape[0]
        
            if response.get('errors'):
                api_run_errors.extend(response.errors)
                api_run_meta['number of errors'] += response.get('errorCount')
            time.sleep(0.4)
        except Exception as error:
            logging.error(f'Error in processing endpoint {endpoint}')

    # fetching the information from the datasets endpoint
    logging.info('Fetching data from datasets')
    response = client.get_dataset_information(provisional_id=provisional_id)
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
    with Client(url=os.getenv('EMX2_HOST'),
                schema= os.getenv('JOBS_SCHEMA'),
                token=os.getenv('EMX2_HOST_TOKEN')) as molgenis:

        molgenis.save_schema(table='Jobs Ega Api', data=api_run_meta_df)

        if api_run_errors:
            molgenis.save_schema(
                table='Job errors', data=api_run_errors)
    
    for key in ega_output_data.keys():        
        # import into the staging area 
        with Client(url=os.getenv('EMX2_HOST'),
                    schema= os.getenv('EMX2_HOST_SCHEMA'),
                    token=os.getenv('EMX2_HOST_TOKEN')) as molgenis:

            molgenis.save_schema(table=key, data=ega_output_data[key])
    
