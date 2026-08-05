"""Create dataset for the CatalogueOntologies Organisations table

To use, find the IDs that correspond to the ROR organisation and paste them below.

Example:

```py
ids_to_map = ['https://ror.org/03cv38k47', 'https://ror.org/012p63287']
main(rorIDs=ids_to_map)
```

Run the script to retrieve data from ROR. A csv file will be generated containing
the ROR metadata. Move the file into the appropriate location and rename (if applicable).
Import the file into the CatalogueOntologies schema.

NOTE: you may need to fix the country mappings as both systems use different terminologies

"""

import requests
import pandas as pd
from tqdm import tqdm

# use for coding ROR countries to MOLGENIS countries
COUNTRY_MAPPINGS = {
    'The Netherlands': 'Netherlands (the)',
    'Türkiye': 'Turkey',
    'United Kingdom': 'United Kingdom of Great Britain and Northern Ireland (the)'
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
    for ror_id in tqdm(rorIDs):
        data = client.get_org(ror_id=ror_id)

        new_entry = {
            'ontologyTermURI': ror_id,
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

    # Set IDs here
    ids_to_map = ['https://ror.org/03cv38k47', 'https://ror.org/012p63287', 'https://ror.org/03a1kwz48', 'https://ror.org/03z77qz90', 
                  'https://ror.org/05wg1m734', 'https://ror.org/013czdx64', 'https://ror.org/05xvt9f17', 'https://ror.org/01n2xwm51',
                  'https://ror.org/05bd7c383', 'https://ror.org/03ccx3r49', 'https://ror.org/0377z4z10', 'https://ror.org/018906e22',
                  'https://ror.org/04n6j6456', 'https://ror.org/02jzt6t86', 'https://ror.org/00ca2c886', 'https://ror.org/05f950310',
                  'https://ror.org/01n9zy652', 'https://ror.org/0125yxn03', 'https://ror.org/03wed5r38', 'https://ror.org/01hxy9878',
                  'https://ror.org/02kqnpp86', 'https://ror.org/00dr28g20', 'https://ror.org/00cv9y106', 'https://ror.org/008x57b05',
                  'https://ror.org/01tevnk56', 'https://ror.org/041x7eh14', 'https://ror.org/00zam0e96', 'https://ror.org/05nsbhw27',
                  'https://ror.org/013meh722', 'https://ror.org/01gckhp53', 'https://ror.org/00t3r8h32', 'https://ror.org/05j1gs298',
                  'https://ror.org/05jmd4043', 'https://ror.org/01jmxt844', 'https://ror.org/00ss42h10', 'https://ror.org/05g2amy04', 
                  'https://ror.org/0590pq693', 'https://ror.org/01d5vx451', 'https://ror.org/01kj2bm70']
    main(rorIDs=ids_to_map)
