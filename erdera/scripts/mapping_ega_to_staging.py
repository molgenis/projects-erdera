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

def upload_files():
    '''Upload the files metadata to the staging area through the API. 
    As long as we don't have access to the EGA API of a dataset, we receive the metadata in ZIP files,
    these ZIPs do not include the files metadata. This is uploaded through the API endpoint'''
    client = EGASubmissionsClient()
    provisional_id = environ['PROVISIONAL_ID']

    files = client.get_datasets_files(provisional_id=provisional_id)
    files_df = pd.DataFrame(files['data'])

    api_run_meta = prepare_run_metadata()

    files_df['added by job'] = api_run_meta['id']

    api_run_meta['total number of files'] = files_df.shape[0]

    api_run_meta['number of errors'] = len(files['errors'])

    if api_run_meta['number of errors'] == 0:
        log.info('No errors detected')
        api_run_meta['ok'] = True

    api_run_errors = []
    files_run_errors = pd.DataFrame(files['errors'])

    if len(files_run_errors):
        files_run_errors['type'] = 'EGA API files retrieval'
        api_run_errors.extend(files_run_errors)

    if api_run_errors:
        api_run_errors = pd.DataFrame(api_run_errors)
        api_run_errors['job'] = api_run_meta['id']

    api_run_meta_df = pd.DataFrame([api_run_meta])
    api_run_meta_df['ok'] = api_run_meta_df['ok'].replace({True:'true', False: 'false'})

    # import datasets into Jobs schema
    log.info("Importing data into the staging area")
    with Client(url=os.getenv('EMX2_HOST'),
                schema= os.getenv('JOBS_SCHEMA'),
                token=os.getenv('EMX2_HOST_TOKEN')) as molgenis:

        molgenis.save_schema(table='Jobs Ega Api', data=api_run_meta_df)

        if api_run_errors:
            molgenis.save_schema(
                table='Job errors', data=api_run_errors)
    
    # import datasets into the staging area 
    with Client(url=os.getenv('EMX2_HOST'),
                schema= os.getenv('EMX2_HOST_SCHEMA'),
                token=os.getenv('EMX2_HOST_TOKEN')) as molgenis:

        molgenis.save_schema(table="files", data=files_df)


def upload_metadata():
    """Retrieve all EGA data"""
    client = EGASubmissionsClient()
    provisional_id = environ['PROVISIONAL_ID']

    datasets = client.get_dataset_information(provisional_id=provisional_id)
    datasets_df = pd.DataFrame([datasets['data']])
    
    studies = client.get_datasets_studies(provisional_id=provisional_id)
    studies_df = pd.DataFrame(studies['data'])

    samples = client.get_datasets_samples(provisional_id=provisional_id)
    samples_df = pd.DataFrame(samples['data'])

    analyses = client.get_datasets_analyses(provisional_id=provisional_id)
    analyses_df = pd.DataFrame(analyses['data'])

    sample_file = client.get_datasets_mappings_sample_file(provisional_id=provisional_id)
    sample_file_df = pd.DataFrame(sample_file['data'])
    
    analysis_sample = client.get_datasets_mappings_analysis_sample(provisional_id=provisional_id)
    analysis_sample_df = pd.DataFrame(analysis_sample['data'])

    study_analysis_sample = client.get_datasets_mappings_study_analysis_sample(provisional_id=provisional_id)
    study_analysis_sample_df = pd.DataFrame(study_analysis_sample['data'])
    
    experiments = client.get_datasets_experiments(provisional_id=provisional_id)
    experiments_df = pd.DataFrame(experiments['data'])
    
    run_sample = client.get_datasets_mappings_run_sample(provisional_id=provisional_id)
    run_sample_df = pd.DataFrame(run_sample['data'])
    
    runs = client.get_datasets_runs(provisional_id=provisional_id)
    runs_df = pd.DataFrame(runs['data'])

    study_experiment_run_sample = client.get_datasets_mappings_study_experiment_run_sample(provisional_id=provisional_id)
    study_experiment_run_sample_df = pd.DataFrame(study_experiment_run_sample['data'])

    files = client.get_datasets_files(provisional_id=provisional_id)
    files_df = pd.DataFrame(files['data'])

    api_run_meta = prepare_run_metadata()

    # add which job added the EGA data to the staging area 
    datasets_df['added by job'] = api_run_meta['id']
    studies_df['added by job'] = api_run_meta['id']
    samples_df['added by job'] = api_run_meta['id']
    analyses_df['added by job'] = api_run_meta['id']
    sample_file_df['added by job'] = api_run_meta['id']
    analysis_sample_df['added by job'] = api_run_meta['id']
    study_analysis_sample_df['added by job'] = api_run_meta['id']
    experiments_df['added by job'] = api_run_meta['id']
    run_sample_df['added by job'] = api_run_meta['id']
    runs_df['added by job'] = api_run_meta['id']
    study_experiment_run_sample_df['added by job'] = api_run_meta['id']
    files_df['added by job'] = api_run_meta['id']

    # update information of the api run 
    api_run_meta['total number of datasets'] = datasets_df.shape[0]
    api_run_meta['total number of analyses'] = analyses_df.shape[0]
    api_run_meta['total number of analysis_samples'] = analysis_sample_df.shape[0]
    api_run_meta['total number of experiments'] = experiments_df.shape[0]
    api_run_meta['total number of run_samples'] = run_sample_df.shape[0]
    api_run_meta['total number of runs'] = runs_df.shape[0]
    api_run_meta['total number of sample_files'] = sample_file_df.shape[0]
    api_run_meta['total number of samples'] = samples_df.shape[0]
    api_run_meta['total number of studies'] = studies_df.shape[0]
    api_run_meta['total number of study_analysis_samples'] = study_analysis_sample_df.shape[0]
    api_run_meta['total number of study_experiment_run_samples'] = study_experiment_run_sample_df.shape[0]
    api_run_meta['total number of files'] = files_df.shape[0]

    # add number of errors
    api_run_meta['number of errors'] = len(datasets['errors']) + \
        len(studies['errors']) + \
        len(samples['errors']) + \
        len(analyses['errors']) + \
        len(sample_file['errors']) + \
        len(analysis_sample['errors']) + \
        len(study_analysis_sample['errors']) + \
        len(experiments['errors']) + \
        len(run_sample['errors']) + \
        len(runs['errors']) + \
        len(study_experiment_run_sample['errors']) + \
        len(files['errors'])

    if api_run_meta['number of errors'] == 0:
        log.info('No errors detected')
        api_run_meta['ok'] = True

    # prepare error messages
    api_run_errors = []
    datasets_run_errors = pd.DataFrame(datasets['errors'])
    studies_run_errors = pd.DataFrame(studies['errors'])
    samples_run_errors = pd.DataFrame(samples['errors'])
    analyses_run_errors = pd.DataFrame(analyses['errors'])
    sample_file_run_errors = pd.DataFrame(sample_file['errors'])
    analysis_sample_run_errors = pd.DataFrame(analysis_sample['errors'])
    study_analysis_sample_run_errors = pd.DataFrame(study_analysis_sample['errors'])
    experiments_run_errors = pd.DataFrame(experiments['errors'])
    run_sample_run_errors = pd.DataFrame(run_sample['errors'])
    runs_run_errors = pd.DataFrame(runs['errors'])
    study_experiment_run_sample_run_errors = pd.DataFrame(study_experiment_run_sample['errors'])
    files_run_errors = pd.DataFrame(files['errors'])

    if len(datasets_run_errors):
        datasets_run_errors['type'] = 'EGA API datasets retrieval'
        api_run_errors.extend(datasets_run_errors)

    if len(studies_run_errors):
        studies_run_errors['type'] = 'EGA API studies retrieval'
        api_run_errors.extend(studies_run_errors)

    if len(samples_run_errors):
        samples_run_errors['type'] = 'EGA API samples retrieval'
        api_run_errors.extend(samples_run_errors)

    if len(analyses_run_errors):
        analyses_run_errors['type'] = 'EGA API analyses retrieval'
        api_run_errors.extend(analyses_run_errors)

    if len(sample_file_run_errors):
        sample_file_run_errors['type'] = 'EGA API sample_file retrieval'
        api_run_errors.extend(sample_file_run_errors)

    if len(analysis_sample_run_errors):
        analysis_sample_run_errors['type'] = 'EGA API analysis_sample retrieval'
        api_run_errors.extend(analysis_sample_run_errors)

    if len(study_analysis_sample_run_errors):
        study_analysis_sample_run_errors['type'] = 'EGA API study_analysis_sample retrieval'
        api_run_errors.extend(study_analysis_sample_run_errors)

    if len(experiments_run_errors):
        experiments_run_errors['type'] = 'EGA API experiments retrieval'
        api_run_errors.extend(experiments_run_errors)

    if len(run_sample_run_errors):
        run_sample_run_errors['type'] = 'EGA API run_sample retrieval'
        api_run_errors.extend(run_sample_run_errors)

    if len(runs_run_errors):
        runs_run_errors['type'] = 'EGA API runs retrieval'
        api_run_errors.extend(runs_run_errors)

    if len(study_experiment_run_sample_run_errors):
        study_experiment_run_sample_run_errors['type'] = 'EGA API study_experiment_run_sample retrieval'
        api_run_errors.extend(study_experiment_run_sample_run_errors)

    if len(files_run_errors):
        files_run_errors['type'] = 'EGA API files retrieval'
        api_run_errors.extend(files_run_errors)

    if api_run_errors:
        api_run_errors = pd.DataFrame(api_run_errors)
        api_run_errors['job'] = api_run_meta['id']

    api_run_meta_df = pd.DataFrame([api_run_meta])
    api_run_meta_df['ok'] = api_run_meta_df['ok'].replace({True:'true', False: 'false'})

    # import datasets into Jobs schema
    log.info("Importing data into the staging area")
    with Client(url=os.getenv('EMX2_HOST'),
                schema= os.getenv('JOBS_SCHEMA'),
                token=os.getenv('EMX2_HOST_TOKEN')) as molgenis:

        molgenis.save_schema(table='Jobs Ega Api', data=api_run_meta_df)

        if api_run_errors:
            molgenis.save_schema(
                table='Job errors', data=api_run_errors)
    
    # import datasets into the staging area 
    with Client(url=os.getenv('EMX2_HOST'),
                schema= os.getenv('EMX2_HOST_SCHEMA'),
                token=os.getenv('EMX2_HOST_TOKEN')) as molgenis:

        molgenis.save_schema(table="dataset", data=datasets_df)
        molgenis.save_schema(table="analyses", data=analyses_df)
        molgenis.save_schema(table="analysis_sample", data=analysis_sample_df)
        molgenis.save_schema(table="experiments", data=experiments_df)
        molgenis.save_schema(table="run_sample", data=run_sample_df)
        molgenis.save_schema(table="runs", data=runs_df)
        molgenis.save_schema(table="sample_file", data=sample_file_df)
        molgenis.save_schema(table="samples", data=samples_df)
        molgenis.save_schema(table="studies", data=studies_df)
        molgenis.save_schema(table="study_analysis_sample", data=study_analysis_sample_df)
        molgenis.save_schema(table="study_experiment_run_sample", data=study_experiment_run_sample_df)
        molgenis.save_schema(table="files", data=files_df)



if __name__ == "__main__":

    upload_metadata()
    
