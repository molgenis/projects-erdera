"""GPAP API """

import logging
import requests
import erdera.clients.gpap.gpap_client_types as gpapTypes

# logging.getLogger("requests").setLevel(logging.WARNING)
logging.captureWarnings(True)
log = logging.getLogger("Molgenis GPAP Pyclient")


class GpapClient:
    """Client for the GPAP API"""

    def __init__(self, api_url: str, token: str, api_page_size: int = 100):
        """Initialize the GPAP client

        :param api_url: root API endpoint
        :type api_url: str

        :param token: authentication token to verify requests
        :type token: str

        :param api_page_size: number of records to return in an API request
        :type api_page_size: int (default: 100)

        """
        self.session = requests.Session()
        self.api_url: str = f"{api_url}/" if api_url.endswith(
            '/') is False else api_url
        self.token: str = token
        self.api_page_size = api_page_size
        self.fields: type.ApiRequestFields = {
            'participants': [], 'experiments': []}

    def _post(self, url: str, headers: gpapTypes.ApiHeaders = None, body: gpapTypes.ApiBody = None):
        """Send a POST request to the GPAP API"""
        response = self.session.post(url, headers=headers, json=body)

        if response.status_code != 200:
            msg: str = f"Failed to fetch data from GPAP API: {response.status_code}-{response.text}"
            log.error(msg)
            raise requests.HTTPError(msg)

        return response.json()

    def _get(self, url: str, headers: gpapTypes.ApiHeaders = None):
        """Send a GET request"""
        response = self.session.get(url, headers=headers)

        if response.status_code != 200:
            msg: str = f"Failed to fetch data from GPAP API: {response.status_code}-{response.text}"
            log.error(msg)
            raise requests.HTTPError(msg)

        return response.json()

    def get_ref_list(self, endpoint=str) -> list[gpapTypes.NameValue]:
        """Generic wrapper for GET requests"""
        headers: gpapTypes.ApiHeaders = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        url: str = f"{self.api_url}datamanagement_service/api/{endpoint}/"
        return self._get(url=url, headers=headers)

    def get_participants(self, page: int = 1) -> gpapTypes.ParticipantsResponse:
        """Get participants from the GPAP API

        :param page: a number indicating which page to retrieve data from as response is paginated
        :type page: int

        :param page_size: number indicating the size of the response
        :type page_size: int

        :returns: records containing a metadata for a subset of participants
        :rtype: ParticipantsResponse
        """
        headers: gpapTypes.ApiHeaders = {
            'Content-Type': 'application/json',
            'Authorization': self.token
        }

        body: gpapTypes.ApiBody = {
            "page": page,
            "pageSize": self.api_page_size,
            "fields": self.fields['participants'],
        }

        url: str = f"{self.api_url}phenostore_service/api/participants_by_exp"
        return self._post(url=url, headers=headers, body=body)

    def get_experiments(self, page: int = 1) -> gpapTypes.ExperimentsResponse:
        """Get experiments from the GPAP API

        :param page: a number indicating which page to retrieve data from as response is paginated
        :type page: int

        :param page_size: number indicating the size of the response
        :type page_size: int

        :returns: records containing a metadata for a subset of experiments
        :rtype: ExperimentsResponse
        """
        headers: gpapTypes.ApiHeaders = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }

        body: gpapTypes.ApiBody = {
            "page": page,
            "pageSize": self.api_page_size,
            "fields": self.fields['experiments']
        }

        url: str = f"{self.api_url}datamanagement_service/api/experimentsview/"
        return self._post(url=url, headers=headers, body=body)

    def get_ref_erns(self) -> list[gpapTypes.NameValue]:
        """Retrieve ERN reference list"""
        return self.get_ref_list('ernlist')

    def get_ref_kits(self) -> list[gpapTypes.NameValue]:
        """Retrieve Kit reference list"""
        return self.get_ref_list('kitlist')

    def get_ref_tissue(self) -> list[gpapTypes.NameValue]:
        """Retrieve Tissues reference list"""
        return self.get_ref_list('tissuelist')
