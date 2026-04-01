"""Map the EGA data from the staging area to RD3"""

import pandas as pd
from dotenv import load_dotenv
from os import environ
load_dotenv()
import re
import logging
from molgenis_emx2_pyclient.client import Client

logging.captureWarnings(True)
log = logging.getLogger("Staging Area Mapping")

def get_staging_area_data(endpoint: str):
    """Retrieve metadata from the staging area (/<staging area>/<endpoint>)"""
    logging.info(f'Retrieving {endpoint} EGA information from staging area')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table=endpoint,
            schema=environ['EMX2_HOST_SCHEMA'],
            as_df=True
        )
    
def add_resources(client: Client): 
    """Create resources table based on datasets and studies from the EGA."""
    ## Add the EGA datasets part of the EGA study as seperate resource entries
    dataset = get_staging_area_data(endpoint='dataset')[['accession_id', 'title', 'description', \
                                                         'num_samples', 'created_at']]
    dataset = dataset.rename(columns={
        'accession_id': 'id',
        'title': 'name',
        'created_at':'start year',
        'num_samples': 'number of participants'
    })

    dataset['website'] = dataset['id'].apply(lambda x: f'https://ega-archive.org/datasets/{x}')
    dataset['type'] = 'Registry'
    dataset['start year'] = dataset['start year'].apply(lambda x: pd.to_datetime(x).year if not pd.isna(x) else x)

    dataset_accession_id = dataset['id'][0]

    # add the EGA study
    study = get_staging_area_data(endpoint='studies')[[
        'accession_id','title','description','created_at']]
    study['created_at'] = pd.to_datetime(study['created_at']).dt.year
    study = study.rename(columns={
        'accession_id': 'id',
        'title': 'name',
        'created_at': 'start year'
    })
    # function variable for 'included in resources'
    study_accession_id = study['id'][0]

    # add website
    study['website'] = f"https://ega-archive.org/studies/{study_accession_id}"

    # add type of resource
    study['type'] = 'Registry'

    # Add number of participants
    # Note: this assumes all datasets in the table are part of this study
    study['number of participants'] = dataset['number of participants'].astype('Int64').sum()

    # add the child networks (the EGA datasets belonging to this study)
    study['child networks'] = ','.join(dataset['id'])

    # concat the resources table to include the EGA study and the dataset(s)
    resources = pd.concat([dataset, study])

    client.save_schema(table='Resources', data=resources)
    return {'dataset_id': dataset_accession_id,
            'study_id': study_accession_id} # return accession IDs
    
def ega_to_files(client: Client, accession_ids: str):
    """Map file metadata from the EGA staging area to RD3's Files"""
    # get EGA files
    files = get_staging_area_data(endpoint='files')[[
        'accession_id', 'unencrypted_checksum', 'unencrypted_checksum_type', 'extension'
    ]]

    # rename columns 
    files = files.rename(columns = {
        'unencrypted_checksum':'checksum', 
        'unencrypted_checksum_type': 'checksum type',
        'extension': 'format'
    })

    # checksum type TODO: move this to the ontology mappings schema
    checksum_dict = {
        'SHA256': 'SHA-256'
    }
    files['checksum type'] = files['checksum type'].replace(checksum_dict)

    ### included in resources 
    # TODO: add the corresponding EGAD number (need to establish link between files and dataset ID)
    files['included in resources'] = accession_ids['study_id']

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
    files['id'] = files['accession_id'].map(individual_file)

    ### individuals
    # map individual using the EGA samples and EGA file_sample endpoint
    samples = get_staging_area_data(endpoint='samples')

    # merge the samples df with the file_sample df to match subject id to an file accession ID 
    merged_df = pd.merge(samples, file_sample, left_on = 'accession_id', right_on='sample_accession_id')
    # create dictionary between subject ID and file accession ID 
    files_subjectID = dict(zip(merged_df['file_accession_id'], merged_df['subject_id']))
    # map individual id 
    files['individuals'] = files['accession_id'].map(files_subjectID)
    
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
    files['produced by experiment'] = files['accession_id'].map(files_experiment)
    # filter out the experiment ID from description 
    def get_exp(description): 
        match = re.search(r"E\d{6}", description)
        return match.group()        
    files['produced by experiment'] = files['produced by experiment'].apply(get_exp)
    
    # drop the accession id 
    files = files.drop(columns=['accession_id'])

   # save
    client.save_schema(table='Files', data=files)
    
if __name__ == "__main__":

    db = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'],
        token=environ['MOLGENIS_TOKEN']
    )

    accession_ids = add_resources(db)
    ega_to_files(db, accession_ids)

