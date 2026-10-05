"""
Map the EGA data from the staging area to RD3

## For deployment

1. Copy the contents of this script into the script editor UI
2. Copy the following requirements into the 'dependencies' field
3. Save and run

### Dependencies

pandas
python-dotenv
molgenis-emx2-pyclient
"""

import asyncio
import logging
import re
import sys
import tempfile
import zipfile
from os import environ
from zipfile import ZipFile

import pandas as pd
from dotenv import load_dotenv
from molgenis_emx2_pyclient.client import Client

load_dotenv()

# set environment variables
MOLGENIS_HOST = 'http://localhost:8080/'
MOLGENIS_TOKEN = environ['MOLGENIS_TOKEN']
SCHEMA_EGA_SOURCE = 'Staging Area Ega'
MOLGENIS_HOST_SCHEMA_TARGET = 'erdera'
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
log = logging.getLogger("Staging Area Mapping EGA")

if len(sys.argv) == 2:
    print("EGA Dataset", sys.argv[1])
    ACCESSION_ID = sys.argv[1]
    log.info('Received arg: Dataset accession ID: %s',
             ACCESSION_ID)
    
if environ.get('PROVISIONAL_ID'):
    ACCESSION_ID = environ['PROVISIONAL_ID']

def get_staging_area_data(endpoint: str):
    """Retrieve metadata from the staging area (/<staging area>/<endpoint>)"""
    log.info('Retrieving %s EGA information from staging area',
                 endpoint)
    with Client(MOLGENIS_HOST, token=MOLGENIS_TOKEN) as client_ind:
        return client_ind.get(
            table=endpoint,
            schema=SCHEMA_EGA_SOURCE,
            as_df=True
        )
    
def get_staging_area_files():
    """Retrieve metadata from the staging area files"""
    log.info('Retrieving files EGA information from staging area')
    with Client(MOLGENIS_HOST, token=MOLGENIS_TOKEN) as client_ind:
        return client_ind.get(
            table='files',
            schema=SCHEMA_EGA_SOURCE,
            as_df=True,
            query_filter=f'dataset_accession_id == {ACCESSION_ID}'
        )
    
def add_collections(client: Client): 
    """Create collections table based on datasets and studies from the EGA."""
    ## Add the EGA datasets part of the EGA study as seperate collection entries
    dataset = get_staging_area_data(endpoint='dataset')[['accession_id', 'title', 'description', 'num_samples', 'created_at']]
    dataset = dataset.rename(columns={
        'accession_id': 'id',
        'title': 'name',
        'created_at':'start year',
        'num_samples': 'number of participants'
    })

    dataset['website'] = dataset['id'].apply(lambda x: f'https://ega-archive.org/datasets/{x}')
    dataset['type'] = 'Registry'
    dataset['start year'] = dataset['start year'].apply(lambda x: pd.to_datetime(x).year if not pd.isna(x) else x)

    # add the EGA study
    study = get_staging_area_data(endpoint='studies')[[
        'accession_id','title','description','created_at']]
    study['created_at'] = pd.to_datetime(study['created_at']).dt.year
    study = study.rename(columns={
        'accession_id': 'id',
        'title': 'name',
        'created_at': 'start year'
    })

    # get the study accession id
    study_accession_id = study['id'][0]
    
    # add website
    study['website'] = f"https://ega-archive.org/studies/{study_accession_id}"

    # add type
    study['type'] = 'Registry'

    # Add number of participants
    # Note: this assumes all datasets in the table are part of this study
    study['number of participants'] = dataset['number of participants'].astype('Int64').sum()

    # create linkage between study and datasets
    create_linkages(client, study=study_accession_id, dataset=dataset['id'])

    # concat the resources table to include the EGA study and the dataset(s)
    collections = pd.concat([dataset, study])

    client.save_table(table='Collections', data=collections)
    
async def upload_files(client: Client):
    """Zip the files and upload to RD3"""
    # get the files df
    files = ega_to_files()

    with tempfile.TemporaryDirectory() as tmp_dir:
        files.to_csv(f'{tmp_dir}/Files.csv', index=False)

        # initialise an archive 
        zip_file_name=f'{tmp_dir}/archive.zip'

        # zip the data
        with ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as my_zip:
            my_zip.write(f'{tmp_dir}/Files.csv', 'Files.csv')
        # upload the zipped file
        await client.upload_file(schema=MOLGENIS_HOST_SCHEMA_TARGET, file_path=zip_file_name)

def ega_to_files():
    """Map file metadata from the EGA staging area to RD3's Files"""
    # get EGA files
    files = get_staging_area_files()[[
        'accession_id', 'unencrypted_checksum', 'unencrypted_checksum_type', 'extension', 'dataset_accession_id'
    ]]

    # rename columns 
    files = files.rename(columns = {
        'unencrypted_checksum':'checksum', 
        'unencrypted_checksum_type': 'checksum type',
        'extension': 'format',
        'dataset_accession_id': 'included in resources',
        'accession_id': 'alternate ids'
    })

    # checksum type TODO: move this to the ontology mappings schema
    checksum_dict = {
        'SHA256': 'SHA-256'
    }
    files['checksum type'] = files['checksum type'].replace(checksum_dict)

    # transform format TODO: move this to the ontology mappings schema
    format_dict = {
        'fastq.gz': 'FASTQ',
        'fq.gz': 'FASTQ',
        'vcf.gz': 'VCF',
        'vcf': 'VCF',
        'bai': 'BAI',
        'bam': 'BAM',
        'ped': 'PED',
        'pdf': 'PDF',
        'json': 'JSON',
        'txt': 'plain text file format',
        'tbi': 'tabix',
        'csv': 'CSV',
        'bw': 'bigWig',
        'bed': 'BED',
        'tsv': 'TSV',
        'cram': 'CRAM',
        'xml': 'XML',
        'gvcf.gz': 'gVCF',
    }
    
    ### format
    files['format'] = files['format'].replace(format_dict)

    ### file name
    # get file name from file_sample EGA endpoint
    file_sample = get_staging_area_data(endpoint='sample_file')

    # create dictionary of file accession id and the file name
    individual_file = dict(zip(file_sample['file_accession_id'], file_sample['file_name']))
    # set file name based on the EGA file accession ID 
    files['id'] = files['alternate ids'].map(individual_file)

    ### individuals
    # map individual using the EGA samples and EGA file_sample endpoint
    samples = get_staging_area_data(endpoint='samples')

    # merge the samples df with the file_sample df to match subject id to an file accession ID 
    merged_df = pd.merge(samples, file_sample, left_on = 'accession_id', right_on='sample_accession_id')
    # create dictionary between subject ID and file accession ID 
    files_subjectID = dict(zip(merged_df['file_accession_id'], merged_df['subject_id']))
    # map individual id 
    files['individuals'] = files['alternate ids'].map(files_subjectID)
    
    ### produced by experiment
    # analyses has the experiment ID in the description
    analyses = get_staging_area_data(endpoint='analyses')
    # analysis_sample has a sample accession id and an analysis accession id necessary to match the experiment to the correct file
    analysis_sample = get_staging_area_data(endpoint='analysis_sample')

    # merge the dataframes to get the file accession id in one df with the experiment ID (as part of the description)
    merged_df = pd.merge(analyses, analysis_sample, left_on = 'accession_id', right_on='analysis_accession_id')
    merged_df_2 = pd.merge(merged_df, file_sample, left_on = 'sample_accession_id', right_on='sample_accession_id')
    # make a dictionary between the file accession id and the experiment id (as part of the description)
    files_experiment = dict(zip(merged_df_2['file_accession_id'], merged_df_2['description']))

    # add the experiment id (as part of the description)
    files['produced by experiment'] = files['alternate ids'].map(files_experiment)
    # filter out the experiment ID from description 
    def get_exp(description): 
        match = re.search(r"E\d{6}", description)
        return match.group()        
    files['produced by experiment'] = files['produced by experiment'].apply(get_exp)
    
    # return 
    return files

def create_linkages(client: Client, study: str, dataset: pd.Series):
    '''
    Create linkages between the EGA study and the EGA dataset(s)
    '''
    linkages = pd.DataFrame({'linked resource': dataset})
    linkages['resource'] = study
    
    # upload to db
    client.save_table(table='Linkages', data=linkages)
    
if __name__ == "__main__":

    db = Client(
        MOLGENIS_HOST,
        schema=MOLGENIS_HOST_SCHEMA_TARGET,
        token=MOLGENIS_TOKEN
        # uncomment when deploying the script remotely
        #job='${jobId}'
    )

    add_collections(client=db)
    asyncio.run(upload_files(client=db))
