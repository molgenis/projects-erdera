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
headers = {'Authorization':'Bearer {}'.format(environ['TOKEN'])}

class EGASubmissionsClient:
    """
    Retrieve metadata from the EGA public API
    """
    def __init__(self):
        self.session = requests.Session()
        self.api_url = environ['API_URL']

    def get(self, url: str = None):
        """wrapper around session.get"""
        try:
            response = self.session.get(url, headers=headers).json()
            return response
        except requests.exceptions.HTTPError as error:
            print(error)
            return None
        
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
        url = f'{self.api_url}'
        return self.get(url) 
    
    def get_studies(self, provisional_id: str=None):
        """Get the submission information"""
        url = f'{self.api_url}/{provisional_id}/studies'
        return self.get(url)
    
    def get_samples(self, provisional_id: str=None):
        """Get the submission information"""
        url = f'{self.api_url}/{provisional_id}/samples'
        return self.get(url)
    
    def get_analyses(self, provisional_id: str=None):
        """Get the submission information"""
        url = f'{self.api_url}/{provisional_id}/analyses'
        return self.get(url)
    
    def get_datasets(self, provisional_id: str=None):
        """Get the submission information"""
        url = f'{self.api_url}/{provisional_id}/datasets'
        return self.get(url)
    
client_ega = EGASubmissionsClient()
client_ega.get_submissions()
    
    

