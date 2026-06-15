"""RD3 Staging area mapping script: mapping experiments from GPAP to RD3
"""
import logging
from os import environ

import pandas as pd
from dotenv import load_dotenv

from molgenis_emx2_pyclient.client import Client

load_dotenv()

logging.captureWarnings(True)
log = logging.getLogger("Staging Area Mapping")

def get_staging_area_experiments():
    """Retrieve metadata from /<staging area>/Experiments"""
    logging.info('Retrieving required metadata')
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='Experiments',
            schema=environ['MOLGENIS_HOST_SCHEMA_SOURCE'],
            as_df=True
        )
    
def add_resources(client: Client):
    """Adding ERDERA and EMX2 API as resources to RD3. This function should be a part of a setting up script"""
    resources = pd.DataFrame({
        'id': ['ERDERA','SOLVE-RD'],
        'name': ['ERDERA', 'SOLVE-RD'],
        'description': ['European Rare Diseases Research Alliance', 'Solving the Unsolved Rare Diseases']
    })

    # save resources
    client.save_schema(table='Resources', data=resources)

def get_mappings_name(rd3_field_name: str):
    """Get the name of the mappings table as it's defined in the ontology mappings schema
    rd3_field_name: (mappings_name, gpap_field_name)"""
    RD3_dict = {
        'library strategy': ('Experiment types', 'library_strategy'),
        'target enrichment kit': ('Kits', 'kit'),
        'library source': ('Library source','library_source'),
        'tissue type': ('Tissue types', 'tissue'),
        'erns': ('Erns', 'erns')
    }
    return RD3_dict.get(rd3_field_name)

def get_data(rd3_name: str):
    '''Get the mappings data'''
    mappings_name = get_mappings_name(rd3_name)[0]
   
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table=mappings_name,
            schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGY_MAPPINGS'],
            as_df=True
        )

def match_ontology(gpap_data: list):
    """Match the GPAP ontology with the RD3's.
        
        gpap_data = the GPAP ontology list
        """
    # get the mappings data (mapping gpap ontology values to RD3)
    mappings = get_data(gpap_data.name)

    # create a dictionary of the incoming value and the new (rd3) value
    mappings_dict = dict(zip(mappings['incoming value'], mappings['new value']))

    # get the unmatched ones
    unique_values = gpap_data.unique()
    unmatched = [x for x in unique_values if x not in mappings_dict]
    unmatched_df = pd.DataFrame({'incoming value': unmatched})
    unmatched_df['source'] = f'datamanagement_service/api/experimentsview/{get_mappings_name(gpap_data.name)[1]}'

    molgenis = Client(
        environ['EMX2_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGY_MAPPINGS'],
        token=environ['EMX2_HOST_TOKEN']
    )

    # upload the values without a match to the ontology mappings schema
    molgenis.save_schema(table=get_mappings_name(gpap_data.name)[0], data=unmatched_df)
    
    return mappings_dict, unmatched

def map_owner_to_organisation(owners: list):
    """Upload the GPAP owners as organisations in CatalogueOntologies"""
    ontologies_client = Client(
        environ['EMX2_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'],
        token=environ['EMX2_HOST_TOKEN']
    )

    organisations = ontologies_client.get(
        table='Organisations', 
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'],
        as_df=True)
    
    # get the new organisations 
    new_organisations = [owner for owner in owners if owner not in organisations['name'].to_list()]
    
    new_organisations_df = pd.DataFrame({'name': new_organisations})

    # upload the new organisations 
    ontologies_client.save_schema(table='Organisations', 
                                  data=new_organisations_df)
    
def upload_samples(client: Client, data: pd.DataFrame):
    """Build and import the sample metadata based on GPAP's experiments. """

    samples_srDNA = data[['tissue', 'Sample_ID', 'Participant_ID', 'ExperimentID']]\
    .rename(columns={
        'tissue': 'tissue type',
        'Participant_ID': 'individuals',
        'ExperimentID': 'id'
    })

    ## map tissue type
    field_name = 'tissue type'
    matches, unmatched = match_ontology(gpap_data=samples_srDNA[field_name])
    samples_srDNA[field_name] = samples_srDNA[field_name].replace(matches)

    tmp = samples_srDNA.loc[samples_srDNA[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    samples_srDNA = samples_srDNA.drop(tmp, axis=0)

    # upload samples
    client.save_schema(table='Samples srDNA', data=samples_srDNA)
    
def upload_srDNA_experiments(client: Client, data: pd.DataFrame):
    """This function maps GPAP experiments to srDNA experiments in RD3"""
    srDNA = data[['ExperimentID', 'LocalExperimentID', 
                           'kit', 
                           'Owner', 
                           'erns', 
                           'project', 'subproject', 
                           'Participant_ID',
                           'library_strategy', 
                           'Sample_ID', 'library_source']]\
    .rename(columns={
        'ExperimentID':'id',
        'LocalExperimentID': 'alternate ids',
        'kit': 'target enrichment kit',
        'Participant_ID': 'individuals',
        'Sample_ID': 'sample',
        'library_strategy': 'library strategy',
        'library_source': 'library source'
        })

    ## map (sub)projects
    # get the resources
    add_resources(client=client)
    
    # combine project and subproject from GPAP to included in resources in RD3
    srDNA.loc[srDNA['project'].isin(['LatinSeq ERDERA', 'RD-Connect ERDERA']), 'project'] = 'ERDERA' # rename ERDERA projects
    srDNA.loc[srDNA['project'].isin(['Solve-RD ERDERA', 'Solve-RD CMS ERDERA', 'RD-Connect Solve-RD ERDERA',
                                    'Consequitur Solve-RD ERDERA', 'NeurOmics Solve-RD ERDERA']), 'project'] = 'SOLVE-RD' # rename SOLVE-RD projects

    # tmp remove subproject for now based on GPAP's comment
    srDNA['subproject'] = None
    srDNA['included in resources'] = srDNA[['project', 'subproject']].apply(
        lambda x: ','.join(x.dropna()), axis=1
        )
    srDNA = srDNA.drop(columns=['project', 'subproject'])

    ## map library strategy
    field_name = 'library strategy'
    matches, unmatched = match_ontology(gpap_data=srDNA[field_name])
    srDNA[field_name] = srDNA[field_name].replace(matches)

    tmp = srDNA.loc[srDNA[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    srDNA = srDNA.drop(tmp, axis=0)

    ## map library source
    field_name = 'library source'
    matches, unmatched = match_ontology(gpap_data=srDNA[field_name])
    srDNA[field_name] = srDNA[field_name].replace(matches)
    
    tmp = srDNA.loc[srDNA[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    srDNA = srDNA.drop(tmp, axis=0)

    ## map affiliated organisations based on erns and owner columns
    owners = srDNA['Owner'].unique().tolist() # gather all unique owners as a list 
    map_owner_to_organisation(owners=owners) # upload the owners as organisations

    field_name = 'erns'
    matches, unmatched = match_ontology(gpap_data=srDNA[field_name])
    srDNA[field_name] = srDNA[field_name].replace(matches)
    
    tmp = srDNA.loc[srDNA[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    srDNA = srDNA.drop(tmp, axis=0)

    srDNA['affiliated organisations'] = None
    for index, row in srDNA.iterrows():
        erns = row['erns']
        owner = row['Owner']
        if not pd.isna(erns):
            srDNA.loc[index, 'affiliated organisations'] = ','.join(str(field) for field in [erns, owner] if pd.notna(field))
    add_organisations_to_individuals(client=client, ind_org_dict=dict(zip(srDNA['individuals'], srDNA['affiliated organisations'])))

    # remove erns and owner columns
    srDNA = srDNA.drop(columns=['erns', 'Owner']) 

    # local experiment id
    srDNA['local experiment id'] = srDNA['id']

    # set sample ID (which is the experiment ID)
    srDNA['sample'] = srDNA['id']
    
    # upload the experiments
    client.save_schema(table='Experiments srDNA', data=srDNA)

def add_organisations_to_individuals(client: Client, ind_org_dict: dict):
    """Add the submitting organisations to the individuals table"""
    individuals = client.get(table='Individuals', as_df=True)
    individuals['affiliated organisations'] = individuals['id'].map(ind_org_dict)
    client.save_schema(table='Individuals', data=individuals)

if __name__ == "__main__":

    experiments = get_staging_area_experiments()

    db = Client(
        environ['EMX2_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'],
        token=environ['EMX2_HOST_TOKEN']
    )

    output_path = environ['OUTPUT_PATH']

    # build and import srDNA experiments and samples
    upload_samples(client=db, data=experiments)
    upload_srDNA_experiments(client=db, data=experiments)