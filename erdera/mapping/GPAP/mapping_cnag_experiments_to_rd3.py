"""RD3 Staging area mapping script: mapping experiments from GPAP to RD3

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
OUTPUT_FILE = environ["OUTPUT_FILE"]

if environ.get('MOLGENIS_HOST'):
    MOLGENIS_HOST = environ['MOLGENIS_HOST']

# set logger
log = logging.getLogger("Staging Area Mapping Participants")
# write logs to an output file instead of to the screen
logging.basicConfig(level='INFO', filename=OUTPUT_FILE)
# set level of the logger of the requests library
logging.getLogger("requests").setLevel(logging.WARNING)
# set level of the logger of the urllib3 library
logging.getLogger("urllib3").setLevel(logging.WARNING)
# make sure warnings from the standard warning module are written to the log file
logging.captureWarnings(True)

def get_staging_area_experiments():
    """Retrieve metadata from /<staging area>/Experiments"""
    log.info('Retrieving required metadata')
    with Client(MOLGENIS_HOST, token=MOLGENIS_TOKEN) as client_ind:
        return client_ind.get(
            table='Experiments',
            schema=SCHEMA_GPAP_SOURCE,
            as_df=True
        )


def add_collections(client: Client):
    """Adding ERDERA and EMX2 API as collections to RD3. This function should be a part of a setting up script"""
    collections = pd.DataFrame({
        'id': ['ERDERA', 'Solve-RD', 'ERDERA_PF1', 'ERDERA_PF2'],
        'name': ['ERDERA', 'Solve-RD', 'ERDERA_PF1', 'ERDERA_PF2'],
        'description': ['European Rare Diseases Research Alliance', 'Solving the Unsolved Rare Diseases', 'Data freeze 1', 'Data freeze 2']
    })

    collections['type'] = 'Registry'

    # save collections
    client.save_table(table='Collections', data=collections)


def get_mappings_name(rd3_field_name: str):
    """Get the name of the mappings table as it's defined in the ontology mappings schema
    rd3_field_name: (mappings_name, gpap_field_name)"""
    RD3_dict = {
        'library strategy': ('Experiment types', 'library_strategy'),
        'library source': ('Library source', 'library_source'),
        'tissue type': ('Tissue types', 'tissue'),
        'affiliated networks': ('Erns', 'erns'),
        'organisations': ('Organisations', 'Owners')
    }
    return RD3_dict.get(rd3_field_name)


def get_data(rd3_name: str):
    '''Get the mappings data'''

    mappings_name = get_mappings_name(rd3_name)[0]

    with Client(MOLGENIS_HOST, token=MOLGENIS_TOKEN) as client_ind:
        return client_ind.get(
            table=mappings_name,
            schema=SCHEMA_ONTOLOGY_MAPPINGS,
            as_df=True
        )


def match_ontology(gpap_data: list):
    """Match the GPAP ontology with the RD3's.

        gpap_data = the GPAP ontology list
        """
    # get the mappings data (mapping gpap ontology values to RD3)
    mappings = get_data(gpap_data.name)

    # create a dictionary of the incoming value and the new (rd3) value
    mappings_dict = dict(
        zip(mappings['incoming value'], mappings['new value']))

    # get the unmatched ones
    unique_values = gpap_data.unique()
    unmatched = [x for x in unique_values if x not in mappings_dict]
    unmatched_df = pd.DataFrame({'incoming value': unmatched})
    unmatched_df[
        'source'] = f'datamanagement_service/api/experimentsview/{get_mappings_name(gpap_data.name)[1]}'

    molgenis = Client(
        MOLGENIS_HOST,
        schema=SCHEMA_ONTOLOGY_MAPPINGS,
        token=MOLGENIS_TOKEN
    )

    # upload the values without a match to the ontology mappings schema
    molgenis.save_table(table=get_mappings_name(
        gpap_data.name)[0], data=unmatched_df)

    return mappings_dict, unmatched


def map_submitter_to_organisation(submitters: list):
    """Upload the GPAP owners (submitters) as organisations in CatalogueOntologies"""
    client = Client(
        MOLGENIS_HOST,
        token=MOLGENIS_TOKEN
    )

    organisations = client.get(
        table='Organisations',
        schema=SCHEMA_ONTOLOGIES,
        as_df=True)

    # get the mappings of the organisations
    organisations_mappings = dict(zip(get_data('organisations')[
                                  'incoming value'], get_data('organisations')['new value']))

    # get the new organisations
    new_organisations = [
        submitter for submitter in submitters if submitter not in organisations['name'].to_list()]

    # for each new gpap submitter, the rd3 organisations needs to be mapped,
    # additionally, they need to be linked to each other.
    new_organisations_df = pd.DataFrame({'name': new_organisations})
    # update the organisations with the parent (i.e., the official institute)
    mapped = organisations['name'].map(organisations_mappings)
    mask = mapped.notna() & (mapped != organisations['parent'])
    changed = organisations.loc[mask].copy()
    changed['parent'] = mapped.loc[changed.index]

    # assign parents for newly created organisations
    new_organisations_df['parent'] = new_organisations_df['name'].map(
        organisations_mappings)  # link the official ror ontology as parent
    
    # add the 'parent' (i.e., the official ror organisation) to the df
    #samples_srDNA['parent_owner'] = samples_srDNA['submitter'].map(organisations_mappings)

    log.info('Adding the following organisation(s): %s',
             new_organisations_df['name'])

    # merge the original organisations (with updated parents) with the new organisations
    organisation_to_upload = pd.concat([changed, new_organisations_df], ignore_index=True)
    # upload the new organisations
    client.save_table(table='Organisations', 
                      schema=SCHEMA_ONTOLOGIES, 
                      data=organisation_to_upload)

    # upload the new organisation to ontology mappings
    new_organisations_df = new_organisations_df.rename(columns={
        'name': 'incoming value',
        'parent': 'new value'
    })
    new_organisations_df['source'] = 'Owner' # GPAP field source
    client.save_table(table='Organisations', 
                      schema=SCHEMA_ONTOLOGY_MAPPINGS, 
                      data=new_organisations_df)

    # return samples_srDNA

def upload_samples(client: Client, data: pd.DataFrame):
    """Build and import the sample metadata based on GPAP's experiments. """

    samples_srDNA = data[['tissue', 'Sample_ID', 'Participant_ID', 'ExperimentID', 'erns', 'Owner']]\
        .rename(columns={
            'tissue': 'tissue type',
            'Participant_ID': 'individuals',
            'ExperimentID': 'id',
            'erns': 'affiliated networks',
            'Owner': 'submitter'
        })

    # map tissue type
    field_name = 'tissue type'
    matches, unmatched = match_ontology(gpap_data=samples_srDNA[field_name])
    samples_srDNA[field_name] = samples_srDNA[field_name].replace(matches)

    tmp = samples_srDNA.loc[samples_srDNA[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # clear values without an RD3 ontology term equivalent.
    samples_srDNA.loc[tmp, field_name] = np.nan

    # map submitter based on Owner field
    # upload the submitters as organisations
    map_submitter_to_organisation(submitters=samples_srDNA['submitter'].unique().tolist())

    # map the affiliated networks (ERNs)
    field_name = 'affiliated networks'
    matches, unmatched = match_ontology(gpap_data=samples_srDNA[field_name])
    samples_srDNA[field_name] = samples_srDNA[field_name].replace(matches)

    tmp = samples_srDNA.loc[samples_srDNA[field_name].isin(
        unmatched)].index
    # clear values without an RD3 ontology term equivalent.
    # remove rows without a RD3 ontology term equivalent
    samples_srDNA.loc[tmp, field_name] = np.nan

    # create mapping per individual of the submitter and affiliated networks
    mapping = (
        samples_srDNA.groupby("individuals")
       .agg({
           "submitter": lambda x: ",".join(pd.unique(x.dropna().astype(str))),
           "affiliated networks": lambda x: ",".join(pd.unique(x.dropna().astype(str)))
       })
       .to_dict("index")
    )

    # upload samples
    client.save_table(table='Samples srDNA', data=samples_srDNA)

    return mapping

def upload_srDNA_experiments(client: Client, data: pd.DataFrame, mapping: dict):
    """This function maps GPAP experiments to srDNA experiments in RD3"""
    srDNA = data[['ExperimentID', 'LocalExperimentID',
                  'kit',
                  'project', 'subproject',
                  'Participant_ID',
                  'library_strategy',
                  'Sample_ID', 'library_source']]\
        .rename(columns={
            'ExperimentID': 'id',
            'LocalExperimentID': 'alternate ids',
            'kit': 'target enrichment kit',
            'Participant_ID': 'individuals',
            'Sample_ID': 'sample',
            'library_strategy': 'library strategy',
            'library_source': 'library source'
        })

    # map (sub)projects
    # get the collections
    add_collections(client=client)

    # combine project and subproject from GPAP to included in resources in RD3
    srDNA['tmp'] = np.where(srDNA['project'].str.contains(
        'Solve-RD', na=False), 'Solve-RD', pd.NA)  # capture the Solve-RD experiments
    srDNA['tmp2'] = np.where(srDNA['project'].str.contains(
        'ERDERA', na=False), 'ERDERA', pd.NA)  # capture the ERDERA experiments
    # rename the freeze information
    srDNA.loc[srDNA['subproject'].str.contains(
        'ERDERA_PF1'), 'subproject'] = 'ERDERA_PF1'
    srDNA.loc[srDNA['subproject'].str.contains(
        r"ERDERA_PF2|TOPFANA_01|TOPFANA_02|TOPFANA_03|TOPFANA_04"), 'subproject'] = 'ERDERA_PF2'

    # merge project and subproject
    srDNA['included in resources'] = srDNA[['tmp', 'tmp2', 'subproject']].apply(
        lambda x: ','.join(pd.unique(x.dropna())), axis=1
    )
    # drop the unused columns
    srDNA = srDNA.drop(columns=['project', 'subproject', 'tmp', 'tmp2'])

    # map library strategy
    field_name = 'library strategy'
    matches, unmatched = match_ontology(gpap_data=srDNA[field_name])
    srDNA[field_name] = srDNA[field_name].replace(matches)

    tmp = srDNA.loc[srDNA[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # clear values without an RD3 ontology term equivalent.
    srDNA.loc[tmp, field_name] = np.nan

    # map library source
    field_name = 'library source'
    matches, unmatched = match_ontology(gpap_data=srDNA[field_name])
    srDNA[field_name] = srDNA[field_name].replace(matches)

    tmp = srDNA.loc[srDNA[field_name].isin(
        unmatched)].index  # get the indices of the unmatched rows
    # clear values without an RD3 ontology term equivalent.
    srDNA.loc[tmp, field_name] = np.nan

    # set submitter
    srDNA["submitter"] = srDNA["individuals"].map(
        lambda x: mapping.get(x, {}).get("submitter")
    )

    # set affiliated networks (ERNs)
    srDNA["affiliated networks"] = srDNA["individuals"].map(
            lambda x: mapping.get(x, {}).get("affiliated networks")
        )
    
    # add the organisations to the individuals table
    asyncio.run(add_organisations_to_individuals(client=client, mapping = mapping))

    # local experiment id
    srDNA['local experiment id'] = srDNA['id']

    # set sample ID (which is the experiment ID for these samples)
    srDNA['sample'] = srDNA['id']

    # upload the experiments
    client.save_table(table='Experiments srDNA', data=srDNA)


async def add_organisations_to_individuals(client: Client, mapping: dict):
    """Add the submitting organisations to the individuals table"""
    individuals = client.get(table='Individuals', as_df=True)
    # set submitter
    individuals["submitter"] = individuals["id"].map(
        lambda x: mapping.get(x, {}).get("submitter")
    )

    # set affiliated networks (ERNs)
    individuals["affiliated networks"] = individuals["id"].map(
            lambda x: mapping.get(x, {}).get("affiliated networks")
        )
    
    #client.save_table(table='Individuals', data=individuals)
    with tempfile.TemporaryDirectory() as tmp_dir:
        individuals.to_csv(f'{tmp_dir}/Individuals.csv', index=False)

        # initialise an archive 
        zip_file_name=f'{tmp_dir}/archive.zip'

        # zip the data
        with ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as my_zip:
            my_zip.write(f'{tmp_dir}/Individuals.csv', 'Individuals.csv')
        # upload the zipped file
        await client.upload_file(schema=MOLGENIS_HOST_SCHEMA_TARGET, file_path=zip_file_name)


if __name__ == "__main__":

    experiments = get_staging_area_experiments()

    db = Client(
        MOLGENIS_HOST,
        schema=MOLGENIS_HOST_SCHEMA_TARGET,
        token=MOLGENIS_TOKEN
    )

    # build and import srDNA experiments and samples
    mapping = upload_samples(client=db, data=experiments)
    upload_srDNA_experiments(client=db, data=experiments, mapping = mapping)