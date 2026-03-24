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
 
def get_staging_area_datasets():
    """Retrieve metadata from /<staging area>/dataset"""
    logging.info('Retrieving dataset EGA information from staging area')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='dataset',
            schema=environ['EMX2_HOST_SCHEMA'],
            as_df=True
        )
    
def get_staging_area_analysis_sample():
    """Retrieve metadata from /<staging area>/analysis_sample"""
    logging.info('Retrieving analysis_sample EGA data from staging area')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='analysis_sample',
            schema=environ['EMX2_HOST_SCHEMA'],
            as_df=True
        )

def get_staging_area_files():
    """Retrieve metadata from /<staging area>/files"""
    logging.info('Retrieving files EGA data from staging area')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='files',
            schema=environ['EMX2_HOST_SCHEMA'],
            as_df=True
        )

def get_staging_area_sample_file():
    """Retrieve metadata from /<staging area>/sample_file"""
    logging.info('Retrieving sample_file EGA data from staging area')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='sample_file',
            schema=environ['EMX2_HOST_SCHEMA'],
            as_df=True
        )

def get_staging_area_samples():
    """Retrieve metadata from /<staging area>/samples"""
    logging.info('Retrieving samples EGA data from staging area')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='samples',
            schema=environ['EMX2_HOST_SCHEMA'],
            as_df=True
        )

def get_staging_area_analyses():
    """Retrieve metadata from /<staging area>/analyses"""
    logging.info('Retrieving analyses EGA data from staging area')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='analyses',
            schema=environ['EMX2_HOST_SCHEMA'],
            as_df=True
        )

def get_staging_area_analysis_sample():
    """Retrieve metadata from /<staging area>/analysis_sample"""
    logging.info('Retrieving analysis_sample EGA data from staging area')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='analysis_sample',
            schema=environ['EMX2_HOST_SCHEMA'],
            as_df=True
        )

def add_resources(client: Client): 
    """Create resources table based on datasets from the EGA."""
    ## Add the EGA datasets part of the EGA study as seperate resource entries
    dataset = get_staging_area_datasets()

    # initialize df
    resources_df = pd.DataFrame()
    # loop through the EGA datasets part of the EGA study
    for _, row in dataset.iterrows():
        # create a new entry for the EGA dataset
        new_entry = {
            'id': row['accession_id'],
            'name': row['title'],
            'description': row['description'],
            'start year': row['created_at'],
            'website': f'https://ega-archive.org/datasets/{row['accession_id']}',
            'type': 'Registry',
            'number of participants': row['num_samples']
        }
        # make df of new entry dict
        new_entry_df = pd.DataFrame([new_entry])
        # get start year from the created_at date
        new_entry_df['start year'] = pd.to_datetime(new_entry_df['start year']).dt.year

        # determine the number of individuals with sample information
        analysis_samples = get_staging_area_analysis_sample()
        
        participants_with_samples = [participant_id for participant_id in analysis_samples['biosample_id']]
        # set the number of participants with samples (with None filtered out)
        num_individuals_with_samples_dataset = len(set(filter(lambda x: x is not pd.NA, participants_with_samples)))
        new_entry_df['number of participants with samples'] = num_individuals_with_samples_dataset

        resources_df = pd.concat([resources_df, new_entry_df])

    client.save_schema(table='Resources', data=resources_df)
    return resources_df['id'][0] # return dataset accession ID 
    
def ega_to_files(client: Client, accession_id: str):
    # get EGA files
    files = get_staging_area_files()[[
        'accession_id', 'unencrypted_checksum', 'unencrypted_checksum_type', 'extension'
    ]]

    # rename columns 
    files = files.rename(columns = {
        'unencrypted_checksum':'checksum', 
        'unencrypted_checksum_type': 'checksum type',
        'extension': 'format'
    })

    # checksum type
    checksum_dict = {
        'SHA256': 'SHA-256'
    }
    files['checksum type'] = files['checksum type'].replace(checksum_dict)

    ### included in resources
    files['included in resources'] = accession_id

    # transform format
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
    file_sample = get_staging_area_sample_file()

    # create dictionary of file accession id and the file name
    individual_file = dict(zip(file_sample['file_accession_id'], file_sample['file_name']))
    # set file name based on the EGA file accession ID 
    files['id'] = files['accession_id'].map(individual_file)

    ### individuals
    # map individual using the EGA samples and EGA file_sample endpoint
    samples = get_staging_area_samples()

    # merge the samples df with the file_sample df to match subject id to an file accession ID 
    merged_df = pd.merge(samples, file_sample, left_on = 'accession_id', right_on='sample_accession_id')
    # create dictionary between subject ID and file accession ID 
    files_subjectID = dict(zip(merged_df['file_accession_id'], merged_df['subject_id']))
    # map individual id 
    files['individuals'] = files['accession_id'].map(files_subjectID)
    
    ### produced by experiment
    # analyses has the experiment ID in the description
    analyses = get_staging_area_analyses()
    # analysis_sample has a sample accession id and an analysis accession id necessary to match the experiment to the correct file
    analysis_sample = get_staging_area_analysis_sample()

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

    accession_id = add_resources(db)
    ega_to_files(db, accession_id)

