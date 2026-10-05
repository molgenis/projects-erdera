"""
This script queries the EGA API to retrieve the dataset information 
according to an endpoint the user can specify.
"""

import logging
from os import environ

import requests
from dotenv import load_dotenv

load_dotenv()

# set environment variables
API_URL = ''
CLIENT_ID = ''
TOKEN_URL = ''

USERNAME = None
PASSWORD = None
if environ.get('USERNAME'):
    USERNAME = environ['USERNAME']
    PASSWORD = environ['PASSWORD']
OUTPUT_FILE = environ['OUTPUT_FILE']

# write logs to an output file instead of to the screen
logging.basicConfig(level='INFO', filename=OUTPUT_FILE)
# set level of the logger of the requests library
logging.getLogger("requests").setLevel(logging.WARNING)
# set level of the logger of the urllib3 library
logging.getLogger("urllib3").setLevel(logging.WARNING)
# make sure warnings from the standard warning module are written to the log file
logging.captureWarnings(True)
# set logger
log = logging.getLogger("EGA API fetcher")

# set authorization token
class EGASubmissionsClient:
    """
    Retrieve metadata from the EGA public API
    """
    def __init__(self):
        self.session = requests.Session()
        self.api_url = API_URL
        self.access_token = None
        self.refresh_token = None
        # initialize token creation
        if USERNAME != None:
            self.get_tokens()

    def get_tokens(self):
        """Get access and refresh token using username and password"""
        data = {
            'grant_type': 'password',
            'client_id': CLIENT_ID,
            'username': USERNAME,
            'password': PASSWORD
        }
        response = requests.post(TOKEN_URL, data=data)
        response.raise_for_status()
        tokens = response.json()
        self.access_token = tokens['access_token']
        self.refresh_token = tokens['refresh_token']
        
    def refresh_access_token(self):
        '''Get new tokens using the refresh token'''
        data = {
            'grant_type': 'refresh_token',
            'client_id': CLIENT_ID,
            'refresh_token': self.refresh_token
        }
        response = requests.post(TOKEN_URL, data=data)
        response.raise_for_status()
        tokens = response.json()
        self.access_token = tokens['access_token']
        self.refresh_token = tokens['refresh_token']

    def get(self, url: str | None = None, include_headers: bool = True):
        """wrapper around session.get"""
        output = {
            'data': [],
            'errors': [] ,
            'errorCount': 0,
        }
        # set the header with the token
        headers = {'Authorization': f'Bearer {self.access_token}'}
        try:
            if include_headers:
                response = self.session.get(url, headers=headers)
            else: 
                response = self.session.get(url)
            response.raise_for_status() # so the error is thrown
            output['data'] = response.json()
            return output
        except requests.exceptions.HTTPError as error:
            if response.status_code == 401: # if token is expired
                log.info('Refreshing authentication tokens')
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
                log.warning(page_error['message'])

                return output
            
    def get_endpoint_dataset(self, provisional_id: str, endpoint: str | None = None, include_headers: bool = True):
        """Get the dataset information with a user-specified endpoint. If no endpoint is given, the generic
        dataset endpoint is called."""
        url = f'{self.api_url}/datasets/{provisional_id}'
        if endpoint:
            url += f'/{endpoint}'
        return self.get(url=url, include_headers=include_headers)
        
    def get_endpoint_studies(self, study_id: str, endpoint: str | None = None):
        """Get the studies information with a user-specified endpoint. If no endpoint is given, the generic 
        studies endpoint is called."""
        url = f'{self.api_url}/studies/{study_id}'
        if endpoint:
            url += f'/{endpoint}'
        return self.get(url=url)