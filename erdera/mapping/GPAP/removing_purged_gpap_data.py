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
import ast
import asyncio
import logging
import tempfile
import zipfile
from os import environ
from zipfile import ZipFile

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
log = logging.getLogger("Purging deleted participants")
# write logs to an output file instead of to the screen
logging.basicConfig(level='INFO', filename=OUTPUT_FILE)
# set level of the logger of the requests library
logging.getLogger("requests").setLevel(logging.WARNING)
# set level of the logger of the urllib3 library
logging.getLogger("urllib3").setLevel(logging.WARNING)
# make sure warnings from the standard warning module are written to the log file
logging.captureWarnings(True)

def get_staging_area_participants():
    """Retrieve metadata from /<staging area>/Participants"""
    log.info('Retrieving required metadata')
    with Client(MOLGENIS_HOST, token=MOLGENIS_TOKEN) as client_ind:
        return client_ind.get(
            table='Participants',
            schema=SCHEMA_GPAP_SOURCE,
            as_df=True
        )

db = Client(
        MOLGENIS_HOST,
        schema=MOLGENIS_HOST_SCHEMA_TARGET,
        token=MOLGENIS_TOKEN,
    )

inds_RD3 = db.get(table='Individuals', columns=['id'], as_df=True)
inds_RD3 = set(inds_RD3['id'])
participants = get_staging_area_participants()
inds_GPAP = set(participants['report_id'])

# The IDs that were removed from the GPAP database
individuals_to_remove = inds_RD3 - inds_GPAP

# delete experiments
to_delete = db.get(table='Experiments srDNA',columns=['id'], query_filter=f'individuals.id == {list(individuals_to_remove)}')
db.delete_records(table='Experiments srDNA', data=to_delete)
log.info('Deleted the following record(s) from Experiments srDNA %s',
         to_delete)

# delete samples
to_delete = db.get(table='Samples srDNA',columns=['id'], query_filter=f'individuals.id == {list(individuals_to_remove)}')
db.delete_records(table='Samples srDNA', data=to_delete)
log.info('Deleted the following record(s) from Samples srDNA %s',
         to_delete)

# delete files
# to_delete = db.get(table='Files',columns=['id', 'individuals'], query_filter=f'individuals.id == {list(individuals_to_remove)}')

async def delete_files():
    for indv in list(individuals_to_remove):
        to_delete = db.get(table='Files',columns=['id', 'individuals', 'format'], query_filter=f'individuals.id == {indv}')
        for entry in to_delete:
            individuals = entry.get('individuals').split(',')
            if len(individuals) > 1:
                id = entry.get('id')
                individuals.remove(indv)
                new_entry = {
                    'id': id,
                    'individuals': ','.join(individuals),
                    'format': entry.get('format')
                }

                with tempfile.TemporaryDirectory() as tmp_dir:
                    pd.DataFrame([new_entry]).to_csv(f'{tmp_dir}/Files.csv', index=False)

                    # initialise an archive 
                    zip_file_name=f'{tmp_dir}/archive.zip'

                    # zip the data
                    with ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as my_zip:
                        my_zip.write(f'{tmp_dir}/Files.csv', 'Files.csv')
                    # upload the zipped file
                    await db.upload_file(schema=MOLGENIS_HOST_SCHEMA_TARGET, file_path=zip_file_name)
                db.save_table(table='Files', data=pd.DataFrame([new_entry]))

asyncio.run(delete_files())

        # update data with single column update
db.delete_records(table='Samples srDNA', data=to_delete)
log.info('Deleted the following record(s) from Samples srDNA %s',
         to_delete)

# variant discussion
# delete variant interpretations
# variant interpretation analyses
# experiments
# samples
# files
# get auto ID from clinc obs
# phenotype observations
# disease history
# consent
# clinical observations
# pedigree members
# if pedigree is now empty, remove pedigree. 





