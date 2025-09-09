"""Retrieve ERNs from ROR and build MOLGENIS-ONTOLOGY dataset"""

from os import path
import requests
import pandas as pd
from tqdm import tqdm

rorApiSession = requests.session()


def get_organisation(session=rorApiSession, _id: str = None):
    """Retrieve ROR Metadata"""
    try:
        url = f"https://api.ror.org/v2/organizations/{_id}"
        response = session.get(url)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as err:
        print(err)
        return None


if __name__ == "__main__":

    # ERNs can be retrieved from the parent organisation: https://ror.org/00r7apq26
    parent_ern = get_organisation(session=rorApiSession, _id='00r7apq26')

    # unpack relationships
    if 'relationships' in parent_ern:
        erns = parent_ern['relationships']
        output = []

        for ern in tqdm(erns):
            if ern.get('type') == 'child':

                # extract initial information
                term = {
                    'name': ern['label'],
                    'codesystem': 'ROR',
                    'code': path.basename(ern['id']),
                    'ontologyTermURI': ern['id'],
                }

                # retrieve metadata about each ERN
                ern_metadata = get_organisation(_id=term['code'])

                if 'types' in ern_metadata:
                    term['type'] = ','.join(ern_metadata['types'])

                if 'locations' in ern_metadata:
                    location = ern_metadata['locations'][0]['geonames_details']
                    term['country'] = location['country_name']
                    term['city'] = location['name']
                    term['latitude'] = location['lat']
                    term['longitude'] = location['lng']

                if 'names' in ern_metadata:
                    for ern_name in ern_metadata['names']:
                        if 'alias' in ern_name.get('types'):
                            term['aliases'] = ern_name['value']

                output.append(term)

        pd.DataFrame(output).to_csv('model/lookups/ERNS.csv', index=False)
