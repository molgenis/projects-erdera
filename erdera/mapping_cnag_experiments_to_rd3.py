"""RD3 Staging area mapping script: mapping experiments from GPAP to RD3
"""
import logging
from os import environ
import pandas as pd
from molgenis_emx2_pyclient.client import Client
from dotenv import load_dotenv
load_dotenv()

logging.captureWarnings(True)
log = logging.getLogger("Staging Area Mapping")

def get_staging_area_experiments():
    """Retrieve metadata from /<staging area>/Experiments"""
    logging.info('Retrieving required metadata')
    with Client(environ['MOLGENIS_HOST'], token=environ['MOLGENIS_TOKEN']) as client_ind:
        return client_ind.get(
            table='Experiments',
            schema=environ['MOLGENIS_HOST_SCHEMA_SOURCE'],
            as_df=True
        )
    
def build_import_NGS_sequencing(client: Client, data: pd.DataFrame):
    """This function maps GPAP experiments to NGS sequencing in RD3"""
    ngs_sequencing = data[['ExperimentID', 'LocalExperimentID', 
                           'kit', 
                        # 'Owner', 
                        #    'erns', 'tissue', 'project', 'subproject', 
                           'Participant_ID',
                        #    'Submitter_ID', # not sure what this is
                        #    'library_strategy', 
                           'Sample_ID']]\
    .rename(columns={
        'ExperimentID':'id',
        'LocalExperimentID': 'alternate ids',
        'kit': 'target enrichment kit',
        'Owner': 'persons involved',
        'erns': 'affiliated organisations',
        'tissue': 'tissue type',
        'project': 'included in resources',
        'subproject': 'included in resources',
        'Participant_ID': 'individuals',
        #'Submitter_ID': '',
        'Sample_ID': 'sample id',
        'library_strategy': 'library strategy'
        })
    #until API call is fixed
    ngs_sequencing['individuals'] = environ['TMP_PARTICIPANT']
    
    client.save_schema(table = 'NGS sequencing', data=ngs_sequencing)

if __name__ == "__main__":

    experiments = get_staging_area_experiments()

    db = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'],
        token=environ['MOLGENIS_TOKEN']
    )

    output_path = environ['OUTPUT_PATH']

    # Map the experiments to NGS sequencing
    build_import_NGS_sequencing(db, experiments)
