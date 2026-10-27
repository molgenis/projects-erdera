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
        token=MOLGENIS_TOKEN
    )

inds_RD3 = db.get(table='Individuals', columns=['id'], as_df=True)
inds_RD3 = set(inds_RD3['id'])
participants = get_staging_area_participants()
inds_GPAP = set(participants['report_id'])

# The IDs that were removed from the GPAP database
individuals_to_remove = inds_RD3 - inds_GPAP

# variant discussion
# delete variant interpretations
# variant interpretation analyses
# experiments
# samples






