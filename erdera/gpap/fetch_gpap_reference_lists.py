"""
Retrieve reference lists of interest from the GPAP datamanagement endpoint.
This includes:

- ERNS
- Tissues
- Kits
"""

import os
import logging
import pandas as pd
from dotenv import load_dotenv
from molgenis_emx2_pyclient import Client
from erdera.clients.gpap.gpap_client_prod import GpapClient
import erdera.clients.gpap.gpap_client_types as types
load_dotenv()

logging.basicConfig(level=logging.INFO)
logging.captureWarnings(True)
log = logging.getLogger("API Client")

if __name__ == '__main__':

    gpap = GpapClient(
        api_url=os.getenv("GPAP_HOST_API"),
        token=os.getenv('GPAP_HOST_TOKEN')
    )

    # create and retrieve reference lists
    log.info('Retrieving erns, kits, and tissue reflists....')
    erns: list[types.NameValue] = gpap.get_ref_erns()
    kits: list[types.NameValue] = gpap.get_ref_kits()
    tissue: list[types.NameValue] = gpap.get_ref_tissue()

    # prepare erns
    erns_df = pd.DataFrame(erns)
    erns_df['source'] = '/api/ernlist/'
    erns_df = erns_df.rename(columns={'name': 'incoming value'})

    # prepare kits
    kits_df = pd.DataFrame(kits)
    kits_df['source'] = '/api/kitlist/'
    kits_df = kits_df.rename(columns={'kit_name': 'incoming value'})

    # prepare tissue data
    tissue_df = pd.DataFrame(tissue)
    tissue_df['source'] = '/api/tissuelist/'
    tissue_df = tissue_df.rename(columns={'name': 'incoming value'})

    with Client(url=os.getenv('EMX2_HOST'),
                schema='Ontology mappings',
                token=os.getenv('EMX2_HOST_TOKEN')) as molgenis:
        molgenis.save_schema('Gpap erns', data=erns_df)
        molgenis.save_schema('Gpap kits', data=kits_df)
        molgenis.save_schema('Gpap tissues', data=tissue_df)
