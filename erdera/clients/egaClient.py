"""
This script queries the EGA API to retrieve the following information: Submission, Study,
Sample (individual), Analysis (CRAM, crai, gVCF, phenopacket), and Dataset.
"""

import logging
import requests
from dotenv import load_dotenv
import pandas as pd
from os import environ

load_dotenv()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("API fetcher")

# set authorization token
class EGASubmissionsClient:
    """
    Retrieve metadata from the EGA public API
    """
    def __init__(self):
        self.session = requests.Session()
        self.api_url = environ['API_URL']
        self.access_token = None
        self.refresh_token = None
        # initialize token creation
        self.get_tokens()

    def get_tokens(self):
        """Get access and refresh token using username and password"""
        data = {
            'grant_type': 'password',
            'client_id': environ['CLIENT_ID'],
            'username': environ['USERNAME'],
            'password': environ['PASSWORD']
        }
        response = requests.post(environ['TOKEN_URL'], data=data)
        response.raise_for_status()
        tokens = response.json()
        self.access_token = tokens['access_token']
        self.refresh_token = tokens['refresh_token']
        
    def refresh_access_token(self):
        '''Get new tokens using the refresh token'''
        data = {
            'grant_type': 'refresh_token',
            'client_id': environ['CLIENT_ID'],
            'refresh_token': self.refresh_token
        }
        response = requests.post(environ['TOKEN_URL'], data=data)
        response.raise_for_status()
        tokens = response.json()
        self.access_token = tokens['access_token']
        self.refresh_token = tokens['refresh_token']

    def get(self, url: str = None):
        """wrapper around session.get"""
        output = {
            'data': [],
            'errors': [] ,
            'errorCount': 0,
        }
        # set the header with the token
        headers = {'Authorization': f'Bearer {self.access_token}'}
        try:
            response = self.session.get(url, headers=headers)
            response.raise_for_status() # so the error is thrown
            output['data'] = response.json()
            return output
        except requests.exceptions.HTTPError as error:
            if response.status_code == 401: # if token is expired
                print('refreshing tokens')
                logger.info('Refreshing authentication tokens')
                self.refresh_access_token()
                # set header with refreshed token
                headers = {'Authorization': f'Bearer {self.access_token}'}     
                response = self.session.get(url, headers=headers)
                output['data'] = response.json()
                return output
            else:
                page_error = {
                'type': f'HTTP: {error.response.status_code}',
                'message': f"Unable to get EGA data {error} (HTTP: {error.response.status_code})"
                }

                output['errorCount'] += 1
                output['errors'].append(page_error)
                logger.warning(page_error['message'])

                return output
        
    def get_record_counts(self, url: str=None):
        """Retrieve EGA-API-Total-Count"""
        try:
            response = self.session.head(url).headers
            return response['EGA-API-Total-Count']
        except requests.exceptions.HTTPError as error:
            print(error)
            return None
    
    def get_submissions(self):
        """Get the submission information"""
        url = f'{self.api_url}/datasets'
        return self.get(url) 
    
    def get_dataset_information(self, provisional_id: str=None):
        """Get the dataset information"""
        url = f'{self.api_url}/datasets/{provisional_id}'
        return self.get(url)
    
    def get_datasets_studies(self, provisional_id: str=None):
        """Get the  information"""
        url = f'{self.api_url}/datasets/{provisional_id}/studies'
        return self.get(url)
    
    def get_datasets_samples(self, provisional_id: str=None):
        """Get the submission information"""
        url = f'{self.api_url}/datasets/{provisional_id}/samples'
        return self.get(url)
    
    def get_datasets_analyses(self, provisional_id: str=None):
        """Get the submission information"""
        url = f'{self.api_url}/datasets/{provisional_id}/analyses'
        return self.get(url)
    
    def get_datasets_files(self, provisional_id: str=None):
        """Get the files of a dataset"""
        url = f'{self.api_url}/datasets/{provisional_id}/files'
        return self.get(url=url)
    
    def get_datasets_mappings_sample_file(self, provisional_id: str=None):
        """Get the mapping between sample and file of a dataset"""
        url = f'{self.api_url}/datasets/{provisional_id}/mappings/sample_file'
        return self.get(url=url)
        
    def get_datasets_mappings_analysis_sample(self, provisional_id: str=None):
        """Get the mapping between sample and the analysis of a dataset"""
        url = f'{self.api_url}/datasets/{provisional_id}/mappings/analysis_sample'
        return self.get(url=url)
    
    def get_datasets_mappings_study_analysis_sample(self, provisional_id: str=None):
        """Get the mapping between sample, the analysis, and the study"""
        url = f'{self.api_url}/datasets/{provisional_id}/mappings/study_analysis_sample'
        return self.get(url=url)
    
    def get_datasets_experiments(self, provisional_id: str=None):
        """Get the experiments"""
        url = f'{self.api_url}/datasets/{provisional_id}/experiments'
        return self.get(url=url)
    
    def get_datasets_runs(self, provisional_id: str=None):
        """Get the runs"""
        url = f'{self.api_url}/datasets/{provisional_id}/runs'
        return self.get(url=url)
    
    def get_datasets_mappings_run_sample(self, provisional_id: str=None):
        """Get the mapping between sample and runs"""
        url = f'{self.api_url}/datasets/{provisional_id}/mappings/run_sample'
        return self.get(url=url)
    
    def get_datasets_mappings_study_experiment_run_sample(self, provisional_id: str=None):
        """Get the mapping between study, experiment, runs, and samples"""
        url = f'{self.api_url}/datasets/{provisional_id}/mappings/study_experiment_run_sample'
        return self.get(url=url)
    
    def get_studies_information(self, study_id: str=None):
        """Get study information"""
        url = f'{self.api_url}/studies/{study_id}'
        return self.get(url=url)
    
    def get_studies_datasets(self, study_id: str=None):
        """Get the datasets of a study"""
        url = f'{self.api_url}/studies/{study_id}/datasets'
        return self.get(url=url)
    
    
    
# client_ega = EGASubmissionsClient()
# client_ega.get_submissions()
# client_ega.get_files(provisional_id='EGAD50000002187')
# client_ega.get_mappings_sample_file(provisional_id='EGAD50000002187')

# # client_ega.get_dataset_mappings_sample_file(provisional_id='EGAD50000002187')
# headers = {'Authorization':'Bearer {}'.format(environ['TOKEN'])}

# session = requests.Session()
# api_url = environ['API_URL']
# provisional_id='EGAD50000002187'
# url = f'{api_url}/{provisional_id}/mappings/analysis_sample'
# url = f'{api_url}/enums/library_sources'

# tmp = session.get(url, headers=headers)
# tmp_df = pd.DataFrame(tmp)
# tmp_df

# requests.get(url, headers=headers)








    

