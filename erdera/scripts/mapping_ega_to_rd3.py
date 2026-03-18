"""Map the EGA data from the staging area to RD3"""

import pandas as pd
from dotenv import load_dotenv
from os import environ
load_dotenv()
from erdera.clients.egaClient import EGASubmissionsClient
import numpy as np
import re
import logging
from molgenis_emx2_pyclient.client import Client

# class EGAToRD3:
# def __init__(self):

logging.captureWarnings(True)
log = logging.getLogger("Staging Area Mapping")
 
def get_staging_area_datasets():
    """Retrieve metadata from /<staging area>/dataset"""
    logging.info('Retrieving required metadata')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='dataset',
            schema=environ['EMX2_HOST_SCHEMA'],
            as_df=True
        )

def add_resources(self): 
    """Create resources table based on study table and the datasets from the EGA."""
    ## Add the EGA datasets part of the EGA study as seperate resource entries
    study_datasets = self.client.get_studies_datasets(study_id=self.study_id)
    dataset = get_staging_area_datasets()

    # initialize df
    resources_df = pd.DataFrame()
    # loop through the EGA datasets part of the EGA study
    for _, row in study_datasets_df.iterrows():
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
        analysis_samples = self.client.get_datasets_mappings_analysis_sample(provisional_id=row['accession_id'])
        analysis_samples_df = pd.DataFrame(analysis_samples)
        participants_with_samples = [participant_id for participant_id in analysis_samples_df['biosample_id']]
        # set the number of participants with samples (with None filtered out)
        num_individuals_with_samples_dataset = len(set(filter(lambda x: x is not None, participants_with_samples)))
        new_entry_df['number of participants with samples'] = num_individuals_with_samples_dataset

        resources_df = pd.concat([resources_df, new_entry_df])
    
    # add the EGA study
    study = self.client.get_studies_information(study_id=self.study_id)
    date_name = 'created_at'
    study_df = pd.DataFrame([study])[[
        'accession_id','title','description',date_name]]
    study_df[date_name] = pd.to_datetime(study_df[date_name]).dt.year
    study_df = study_df.rename(columns={
        'accession_id': 'id',
        'title': 'name',
        date_name: 'start year'
    })
    # function variable for 'included in datasets'
    self.study_accession_id = study_df['id'][0]

    # add website
    study_df['website'] = f"https://ega-archive.org/studies/{self.study_accession_id}"

    # add type of resource
    study_df['type'] = 'Registry'

    # Add number of participants
    study_df['number of participants'] = resources_df['number of participants'].sum()
    # individuals = self.client.get_datasets_samples(provisional_id=self.provisional_id)
    # study_df['number of participants'] = len(individuals)

    # add number of participants with samples (using biosample_id - todo: needs to be discussed)
    study_df['number of participants with samples'] = resources_df['number of participants with samples'].sum()

    # add the child networks (the EGA datasets belonging to this study)
    study_df['child networks'] = ','.join(resources_df['id'])

    # concat the resources table to include the EGA study and the dataset(s)
    resources_df = pd.concat([resources_df, study_df])

    resources_df.to_csv(f'{self.output_path_dir}/Resources.csv', index=False)

def ega_to_files(self):
    # get EGA files
    files = self.client.get_datasets_files(provisional_id=self.provisional_id)
    files_df = pd.DataFrame(files)[[
        'accession_id', 'unencrypted_checksum', 'unencrypted_checksum_type', 'extension'
    ]]

    # check if the checksum type is MD5, if not, set to NA
    files_df['unencrypted_checksum'] = files_df.apply(lambda row: row['unencrypted_checksum'] if row['unencrypted_checksum_type'] == 'MD5' or row['unencrypted_checksum_type'] == 'SHA256' else np.nan, 
    axis=1)
    # delete checksum type column 
    files_df = files_df.drop('unencrypted_checksum_type', axis = 1)

    # rename columns 
    files_df = files_df.rename(columns = {
        'unencrypted_checksum': 'md5 checksum', 
        'extension': 'format',
        'accession_id': 'id'
    })

    ### included in resources
    files_df['included in resources'] = self.study_accession_id

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
        'gvcf.gz': 'gVCF'
    }
    
    ### format
    files_df['format'] = files_df['format'].map(format_dict)

    ### file name
    # get file name from file_sample EGA endpoint
    file_sample = self.client.get_datasets_mappings_sample_file(provisional_id=self.provisional_id)
    file_sample_df = pd.DataFrame(file_sample)

    # create dictionary of file accession id and the file name
    individual_file = dict(zip(file_sample_df['file_accession_id'], file_sample_df['file_name']))
    # set file name based on the EGA file accession ID 
    files_df['id'] = files_df['id'].map(individual_file)

    ### individuals
    # map individual using the EGA samples and EGA file_sample endpoint
    samples = self.client.get_datasets_samples(provisional_id=self.provisional_id)
    samples_df = pd.DataFrame(samples)

    # merge the samples df with the file_sample df to match subject id to an file accession ID 
    merged_df = pd.merge(samples_df, file_sample_df, left_on = 'id', right_on='sample_accession_id')
    # create dictionary between subject ID and file accession ID 
    files_subjectID = dict(zip(merged_df['file_accession_id'], merged_df['subject_id']))
    # map individual id 
    files_df['individuals'] = files_df['id'].map(files_subjectID)
    
    ### produced by experiment
    # analyses has the experiment ID in the description
    analyses = self.client.get_datasets_analyses(provisional_id=self.provisional_id)
    analyses_df = pd.DataFrame(analyses)
    # analysis_sample has a sample accession id and an analysis accession id necessary to match the experiment to the correct file
    analysis_sample = self.client.get_datasets_mappings_analysis_sample(provisional_id=self.provisional_id)
    analysis_sample_df = pd.DataFrame(analysis_sample)

    # merge the dataframes to get the file accession id in one df with the experiment ID (as part of the description)
    merged_df = pd.merge(analyses_df, analysis_sample_df, left_on = 'id', right_on='analysis_accession_id')
    merged_df_2 = pd.merge(merged_df, file_sample_df, left_on = 'sample_accession_id', right_on='sample_accession_id')
    # make a dictionary between the file accession id and the experiment id (as part of the description)
    files_experiment = dict(zip(merged_df_2['file_accession_id'], merged_df_2['description']))

    # add the experiment id (as part of the description)
    files_df['produced by experiment'] = files_df['id'].map(files_experiment)
    # filter out the experiment ID from description 
    def get_exp(description): 
        match = re.search(r"E\d{6}", description)
        return match.group()        
    files_df['produced by experiment'] = files_df['produced by experiment'].apply(get_exp)
    
    ## add biosamples
    # files_df['biosamples'] = files_df['individuals'].map(self.biosample_individual)

    ## add path of name and format (so the same as name) to do: adjust path once data is on cluster
    # files_df['path'] = files_df['name']

    # write to csv
    files_df.to_csv(f'{self.output_path_dir}/Files.csv', index=False)
