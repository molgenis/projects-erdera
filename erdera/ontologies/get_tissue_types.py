"""Retreive tissue types

This script retrieves tissue types from GTex portal and converts
it into EMX2 ontology format
"""

import requests
import pandas as pd
from tqdm import tqdm


def get_gtex_tissue_types():
    """Retreive tissue type entries metadata"""
    url = 'https://gtexportal.org/api/v2/dataset/tissueSiteDetail?datasetId=gtex_v10&page=0&itemsPerPage=250'
    gtex = requests.Session()
    response = gtex.get(url)
    response.raise_for_status()
    return response.json()


def get_ebi_term_meta(session, code: str = None, mapping_term: str = None):
    """Retrieve ontology term metadata"""
    encoded_iri = 'http%253A%252F%252Fwww.ebi.ac.uk%252Fefo%252F' \
        if 'EFO:' in code \
        else 'http%253A%252F%252Fpurl.obolibrary.org%252Fobo%252F'
    url = ''.join([
        'https://www.ebi.ac.uk/ols4/api/ontologies/',
        code.split(':')[0].lower(),
        '/terms/',
        encoded_iri,
        code.replace(':', '_')
    ])
    response = session.get(url)
    response.raise_for_status()

    response_data = response.json()
    return {
        'mapping_term': mapping_term,
        'name': response_data.get('label'),
        'codesystem': response_data.get('ontology_prefix'),
        'code': response_data.get('obo_id'),
        'ontologyTermURI': response_data.get('ontology_iri'),
        'definition': ' '.join(response_data.get('description'))
    }


if __name__ == '__main__':

    # retrieve tissue types and create data structure
    data = pd.DataFrame(get_gtex_tissue_types()['data'])
    tissues_df = data[[
        'tissueSiteDetailId',
        'tissueSiteDetail',
        'datasetId',
        'ontologyId',
        'ontologyIri'
    ]]

    # retrieve ontology metadata from each tissue
    ebi = requests.Session()
    ontology = []
    for row in tqdm(tissues_df[['ontologyId', 'tissueSiteDetail']].to_dict('records')):
        term_entry = get_ebi_term_meta(
            session=ebi,
            code=row['ontologyId'],
            mapping_term=row['tissueSiteDetail']
        )
        ontology.append(term_entry)

    # prepare ontology table
    ontology_df = pd.DataFrame(ontology)
    ontology_df = ontology_df.sort_values('name')
    ontology_df = ontology_df.reset_index()
    ontology_df['order'] = ontology_df.index

    ontology_df['definition'] = ontology_df[['definition', 'mapping_term']] \
        .agg(' GTex: '.join, axis=1)

    # save data
    ontology_df[
        ['order', 'name', 'codesystem', 'code', 'ontologyTermURI', 'definition']
    ] \
        .to_csv('../../model/lookups/tissue types.csv', index=False)

    ontology_df[['mapping_term', 'name']].to_csv(
        '../../model/tissue mappings.csv', index=False)
