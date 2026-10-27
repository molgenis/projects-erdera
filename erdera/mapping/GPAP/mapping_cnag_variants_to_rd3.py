"""Mapping GPAP Variants data to RD3

## For deployment

1. Copy the contents of this script into the script editor UI
2. Copy the following requirements into the 'dependencies' field
3. Save and run

### Dependencies

numpy
pandas
python-dotenv
molgenis-emx2-pyclient

"""
import logging
from os import environ
import ast

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from molgenis_emx2_pyclient.client import Client

load_dotenv()

# set environment variables
MOLGENIS_HOST = 'http://localhost:8080/'
MOLGENIS_TOKEN = environ['MOLGENIS_TOKEN']
SCHEMA_GPAP_SOURCE = 'Staging Area Gpap'
SCHEMA_ONTOLOGY_MAPPINGS = 'Ontology mappings'
SCHEMA_ONTOLOGIES = 'CatalogueOntologies'
MOLGENIS_HOST_SCHEMA_TARGET = 'erdera'
SCHEMA_QUALITY_CONTROL = 'Quality Control'
OUTPUT_FILE = environ["OUTPUT_FILE"]

if environ.get('MOLGENIS_HOST'):
    MOLGENIS_HOST = environ['MOLGENIS_HOST']

# set logger
log = logging.getLogger("Staging Area Mapping Variants")
# write logs to an output file instead of to the screen
logging.basicConfig(level='INFO', filename=OUTPUT_FILE)
# set level of the logger of the requests library
logging.getLogger("requests").setLevel(logging.WARNING)
# set level of the logger of the urllib3 library
logging.getLogger("urllib3").setLevel(logging.WARNING)
# make sure warnings from the standard warning module are written to the log file
logging.captureWarnings(True)

def get_staging_area_regions():
    """Retrieve metadata from /<staging area>/Participants"""
    log.info('Retrieving required metadata')
    with Client(MOLGENIS_HOST, token=MOLGENIS_TOKEN) as client_ind:
        participants = client_ind.get(
            table='Participants',
            schema=SCHEMA_GPAP_SOURCE,
            as_df=True
        )

    # get the region field
    regions = participants[['regions', 'report_id']]

    regions = regions.dropna() # drop empty records

    # convert to list
    regions['regions'] = regions['regions'].apply(ast.literal_eval)
    # explode the list to have each element as a row
    tmp = regions.explode('regions').reset_index(drop=True)
    result = pd.concat(
        [tmp.drop(columns='regions'),
        pd.json_normalize(tmp['regions'])],
        axis=1
    )

    # explode the list of variants to have each element as a row
    regions = result.explode('variants', ignore_index=True)
    regions = pd.concat(
        [
            regions.drop(columns='variants'),
            pd.json_normalize(regions['variants'])
        ],
        axis=1
    )

    return regions  

def build_import_variants(client: Client, regions: pd.DataFrame):
    # missing: Gene HGVS, gDNA, cDNA, protein, 
    genomic_variants = regions[['sv_type', 'associated_genes', 'region', 'status', 'start_pos', 'chromosome', 'end_pos']].rename(columns={
                 'sv_type': 'variant type',
                 'associated_genes': 'gene id',
                 'region': 'chromosomal region',
                 'status': '',
                 'start_pos': 'start position',
                 'chromosome': 'chromosome',
                 'end_pos': 'stop position'
                 #'timestamp': ''
             })

    genomic_variants["id"] = (
        genomic_variants[["chromosomal region", "variant type"]]
        .fillna("")
        .agg("_".join, axis=1)
        .str.strip("_")
        )

    genomic_variants['start position'] = genomic_variants['start position'].astype('Int64')
    genomic_variants['stop position'] = genomic_variants['stop position'].astype('Int64')
    
    genomic_variants.drop_duplicates().to_csv('', index=False)
    # TODO: check out these duplicates to see if they are about the same variant or not 

def build_import_var_analyses(client: Client, regions: pd.DataFrame):
    # create variant interpretation analyses
    var_int_ana = regions[['sample', 'report_id']].rename(columns={
        'sample': 'experiment',
        'report_id': 'individuals'
    })

    var_int_ana['individuals'].rename('id').drop_duplicates().to_csv('', index=False)
    var_int_ana['experiment'].rename('id').drop_duplicates().to_csv('', index=False)
    
    var_int_ana.to_csv('', index=False)

    # upload and download

    with_IDs = client.get(table='Variant interpretation analyses', as_df=True)
    id_auto = dict(zip(with_IDs['individuals'], with_IDs['id']))

def build_import_var_interpretations(client: Client, regions: pd.DataFrame, id_auto: dict):
    variant_interpretations = regions[['transcript', 'zygosity', 'inheritance', 'significance', 'status', 'region', 'sv_type', 'report_id']].rename(columns={
        #'significance': 'classification',
        'report_id': 'individual'
    })

    variant_interpretations['variant'] = (
         variant_interpretations[["region", "sv_type"]]
        .fillna("")
        .agg("_".join, axis=1)
        .str.strip("_")
        )
    
    variant_interpretations= variant_interpretations.drop(columns=['region', 'sv_type'])
    
    variant_interpretations['interpretation analysis'] = variant_interpretations['individual'].map(id_auto)

    # map inheritance
    inheritance_dict = {
        'Autosomal recessive': 'autosomal recessive inheritance',
        'Autosomal dominant': 'autosomal dominant inheritance',
        'X-linked recessive': 'X-linked recessive inheritance', 
        'X-linked dominant': 'X-linked dominant inheritance',
        'X-linked': 'X-linked inheritance'
    }

    variant_interpretations['inheritance'] = variant_interpretations['inheritance'].map(inheritance_dict)

    # map zygosity
    variant_interpretations['zygosity'] = variant_interpretations['zygosity'].str.lower()

    # to be discussed
    variant_interpretations = variant_interpretations.drop(columns=['status'])

    variant_interpretations.to_csv('', index=False)

    # upload and download for auto ID
    with_IDs = client.get(table='Variant interpretations', as_df=True)
    id_auto = dict(zip(with_IDs['individual'], with_IDs['id']))

def build_import_var_discussions(client: Client, regions: pd.DataFrame, id_auto: dict):
    var_disc = regions[['report_id', 'significance', 'comments']].rename(columns={
        'significance': 'classification',
        'comments': 'summary'
    })

    var_disc['variant interpretation'] = var_disc['report_id'].map(id_auto)
    var_disc = var_disc.drop(columns=['report_id'])

    # map classification
    classification_dict = {
        'Uncertain significance': 'Variant of unknown significance',
        'Other': np.nan
    }
    var_disc['classification'] = var_disc['classification'].replace(classification_dict)

    var_disc.to_csv('', index=False)

if __name__ == "__main__":

    regions = get_staging_area_regions()

    db = Client(
        MOLGENIS_HOST,
        schema=MOLGENIS_HOST_SCHEMA_TARGET,
        token=MOLGENIS_TOKEN
    )

    build_import_variants(client=db, data=regions)
    id_auto = build_import_var_analyses(client=db, regions=regions)
    id_auto = build_import_var_interpretations(client=db, regions=regions, id_auto=id_auto)
    build_import_var_discussions(client=db, regions=regions, id_auto=id_auto)