"""RD3 Staging area mapping scripts
"""
import logging
from os import environ
import pandas as pd
from molgenis_emx2_pyclient.client import Client
from dotenv import load_dotenv
load_dotenv()

logging.captureWarnings(True)
log = logging.getLogger("Staging Area Mapping")


def get_staging_area_participants():
    """Retrieve metadata from /<staging area>/Participants"""
    logging.info('Retrieving required metadata')
    with Client(environ['MOLGENIS_HOST'], token=environ['MOLGENIS_TOKEN']) as client_ind:
        return client_ind.get(
            table='Participants',
            schema=environ['MOLGENIS_HOST_SCHEMA_SOURCE'],
            as_df=True
        )


def build_import_pedigree_table(client, data: pd.DataFrame):
    """Map staging area data into the Pedigree table format"""
    pedigree = data[['famid']] \
        .drop_duplicates() \
        .rename(columns={'famid': 'id'}) \
        .sort_values(by="id")
    client.save_schema(table='Pedigree', data=pedigree)

def build_import_individuals_table(client, data: pd.DataFrame):
    """Map staging area data into the Individuals table"""
    individuals = data[['id', 'sex', 'lifeStatus', 'report_date', 'last_modification_date', 'report_id']]
    individuals = individuals.rename(columns={
        'id': 'alternate ids', 
        'sex': 'gender at birth',
        'lifeStatus': 'individual status', 
        'report_date': 'date created at source', 
        'last_modification_date': 'date updated at source', 
        'report_id': 'id'
    })
    
    # map sex
    gender_dict = {
        'M': 'assigned male at birth', 
        'F': 'assigned female at birth'
    }

    individuals['gender at birth'] = individuals['gender at birth'].map(gender_dict)

    client.save_schema(table='Individuals', data=individuals)



if __name__ == "__main__":

    participants = get_staging_area_participants()

    db = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'],
        token=environ['MOLGENIS_TOKEN']
    )

    # 1. Pedigree table mapping
    build_import_pedigree_table(db, participants)

    # 2. Individuals table mapping
    build_import_individuals_table(db, participants)
    
    # 3. Pegidgree Members mapping

    # 4. Clinical Observations mapping

    # 5. Individual Consent mappings

    # 6. Disease History mapping

    # 7. Phenotype Observations mapping
