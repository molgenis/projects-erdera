"""Compile the organisations ontology table using ROR API
# FILE: ror.py
# AUTHOR: David Ruvolo
# CREATED: 2024-10-02
# MODIFIED: 2026-08-03
# PURPOSE: pull metadata from ROR by ROR ID
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: NA
"""

import requests
import pandas as pd
from tqdm import tqdm


# use for coding ROR countries to MOLGENIS countries
COUNTRY_MAPPINGS = {
    'The Netherlands': 'Netherlands (the)'
}


class RorClient:
    """Retrieve metadata from ROR Api v2"""

    def __init__(self):
        self.api = 'https://api.dev.ror.org/v2/organizations'
        self.session = requests.Session()

    def _get(self, url: str = None, **kwargs):
        """wrapper for session.get"""
        response = self.session.get(url, **kwargs)
        response.raise_for_status()
        if 'errors' in response:
            raise requests.exceptions.HTTPError(
                'Error in request', str(response['errors']))
        return response.json()

    def get_org(self, ror_id: str = None):
        """
        Retrieve metadata for an organisation by ROR ID.

        :param ror_id: ROR identifier for an organisation
        :type ror_id: string

        :returns: object containing metadata about an organisation 
        """
        url: str = f"{self.api}/{ror_id}"
        return self._get(url)


def main(rorIDs: list[str]):
    """From a list of codes, retrieve metadata"""
    client = RorClient()

    dataset = []
    for rorId in tqdm(rorIDs):
        data = client.get_org(ror_id=rorId)

        new_entry = {
            'ontologyTermURI': rorId,
            'codesystem': 'ROR'
        }

        # set Ontology$name (using 'ror_display')
        new_entry['name'] = ';'.join([
            row['value']
            for row in data['names']
            if 'ror_display' in row['types']
        ])

        # Set ontology$acronym
        new_entry['acronym'] = ';'.join([
            row['value']
            for row in data['names']
            if 'acronym' in row['types']
        ])

        # set ontology$website
        if data.get('links'):
            for link in data['links']:
                if link['type'] == 'website':
                    new_entry['website'] = link['value']

        # set geodata
        if data.get('locations'):
            for location in data['locations']:
                print(location)
                if 'geonames_details' in location:
                    geonames = location['geonames_details']
                    new_entry['city'] = geonames.get(
                        'country_subdivision_name')
                    new_entry['latitude'] = geonames.get(
                        'lat')
                    new_entry['longitude'] = geonames.get(
                        'lng')
                    if geonames.get('country_name'):
                        country_name = geonames['country_name']
                        new_entry['country'] = country_name
                        if country_name in COUNTRY_MAPPINGS:
                            new_entry['country'] = COUNTRY_MAPPINGS[country_name]

        # set ontology$type
        new_entry['type'] = ','.join([_type for _type in data.get('types')])

        # add to dataset
        dataset.append(new_entry)
    pd.DataFrame(dataset).to_csv('ror_organisations.csv', index=False)


if __name__ == '__main__':

    # Set IDs here (demo: UMCG + RUG)
    ids_to_map = ['https://ror.org/03cv38k47', 'https://ror.org/012p63287']
    main(rorIDs=ids_to_map)
