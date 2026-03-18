"""RD3 Staging area mapping script: mapping experiments from GPAP to RD3
"""
import logging
from os import environ
import pandas as pd
from molgenis_emx2_pyclient.client import Client
from molgenis_emx2_pyclient.exceptions import PyclientException
import asyncio
import zipfile
from zipfile import ZipFile
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
    
def map_individuals(data:pd.DataFrame, client: Client):
    """Map the individuals based on the experiments -- temporary until the GPAP API is fixed"""
    # get current list of individuals 
    individuals = client.get(table='Individuals', as_df=True)
    ids = individuals['id']

    # gather the individuals that are not already in the individuals RD3 table
    new_individuals = data.loc[~data['Participant_ID'].isin(ids), 'Participant_ID'].rename('id')

    client.save_schema(table='Individuals', data= new_individuals.drop_duplicates())

def add_resources():
    """Adding ERDERA as a resource to RD3. This function should be a part of a setting up script"""
    resources = pd.DataFrame({
        'id': ['ERDERA','EMX2 API'],
        'name': ['ERDERA', 'EMX2 API'],
        'type': ['Rare disease', 'Other type'],
        'description': ['', 'The purpose of this is to test EMX2 API calls']
    })

    # save resources
    resources.to_csv(f'{environ['OUTPUT_PATH']}Resources.csv', index=False)

def make_endpoint():
    """This function makes an endpoint necessary for the mapping. This function should be a part of a setting up script"""
    # disabled
    endpoint = {'id':'main_fdp', 
                'type': 'https://w3id.org/fdp/fdp-o#MetadataService,http://www.w3.org/ns/dcat#Resource,http://www.w3.org/ns/dcat#DataService,https://w3id.org/fdp/fdp-o#FAIRDataPoint',
                'name': "MOLGENIS Fair Data Point",
                'version':'v1.2',
                'description':'MOLGENIS FDP Endpoint for the catalogue data model',
                'publisher':'MOLGENIS',
                'language':'https://www.loc.gov/standards/iso639-2/php/langcodes_name.php?iso_639_1=en',
                'license':'https://www.gnu.org/licenses/lgpl-3.0.html#license-text',
                'conformsTo':'https://specs.fairdatapoint.org/fdp-specs-v1.2.html',
                'metadataCatalog':'EMX2 API',
                'conformsToFdpSpec':'https://specs.fairdatapoint.org/fdp-specs-v1.2.html'}
    
    # save endpoint
    pd.DataFrame([endpoint]).to_csv(f'{environ['OUTPUT_PATH']}Endpoint.csv', index=False)

def make_agent(): 
    """This function makes an agent 'molgenis'. This function should be a part of a setting up script"""
    # disabled
    agent = {
        'name': 'MOLGENIS',
        'logo': 'https://molgenis.org/assets/img/logo_green.png',
        'url': 'https://molgenis.org/',
        'mbox': 'support@molgenis.org',
        'mg_draft': 'FALSE'
    }

    # save agent
    pd.DataFrame([agent]).to_csv(f'{environ['OUTPUT_PATH']}Agent.csv', index=False)

async def upload_curation(client: Client):
    """Upload Resources, Agent, and Endpoint"""
    # make a name for the zipped folder
    zip_file_name=f'{environ['OUTPUT_PATH']}archive.zip'
    # zip the data
    with ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as my_zip:
        my_zip.write(f'{environ['OUTPUT_PATH']}Agent.csv', 'Agent.csv')
        my_zip.write(f'{environ['OUTPUT_PATH']}Endpoint.csv', 'Endpoint.csv')
        my_zip.write(f'{environ['OUTPUT_PATH']}Resources.csv', 'Resources.csv')
    # upload the zipped file with the molgenis schema and the molgenis members
    await client.upload_file(schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'], file_path=zip_file_name)

async def match_ontology(ontology: str, gpap_data: list):
    """Match the GPAP ontology with the RD3's.
        
        ontology = the name of the RD3 ontology
        gpap_data = the GPAP ontology list (with the names)"""
    molgenis = Client(
        environ['MOLGENIS_HOST'],
        schema='CatalogueOntologies',
        token=environ['MOLGENIS_TOKEN']
    )
    # get RD3 ontology and overrides
    rd3_ontology = molgenis.get(
        table=ontology, schema='CatalogueOntologies', as_df=True)

    hpo_overrides = molgenis.get(
        table='Ontology mappings',
        schema=environ['MOLGENIS_HOST_SCHEMA_SOURCE'],
        as_df=True
    )
    hpo_overrides = hpo_overrides.loc[hpo_overrides['ontology'] == ontology]

    # save the matches (the once without a perfect name match)
    matches = {}
    # and save the ones that do not have a name and/or code match
    unmatched = []
    no_correct_name = []
    for ontology_term in gpap_data:
        # Case 1: name match ignore (we only want mismatched names)
        if ontology_term in rd3_ontology['name'].values:
            continue
        # Case 2: if the code is not known and has an override
        elif ontology_term in hpo_overrides['invalid name'].values:
            correct_name = hpo_overrides.loc[hpo_overrides['invalid name']
                                                  == ontology_term, 'correct name']
            if pd.notna(correct_name.iloc[0]): # check if there is an overrride
                matches[ontology_term] = correct_name.squeeze()
            else:
                no_correct_name.append({
                    'ontology': ontology,
                    'invalid name': ontology_term
                })
        # Case 3: no match at all
        else:
            unmatched.append({
                'ontology': ontology,
                'invalid name': ontology_term
            })

    # upload the new unmatched values
    output_path = environ['OUTPUT_PATH']
    pd.DataFrame(unmatched).drop_duplicates().to_csv(f'{output_path}Ontology mappings.csv', index=False)
    await molgenis.upload_file(file_path=f'{output_path}Ontology mappings.csv', schema=environ['MOLGENIS_HOST_SCHEMA_SOURCE'])

    # combine the new unmatched values with the already known unmatched values (but without a correct name)
    # -- these will be removed from the rd3 dataset to be uploaded
    unmatched = unmatched + no_correct_name

    # only return the unique values 
    unmatched = [dict(t) for t in {tuple(unmatched_dict.items()) for unmatched_dict in unmatched}]

    return matches, unmatched

def map_owner_to_organisation(owners: list):
    """Upload the GPAP owners as organisations in CatalogueOntologies"""
    ontologies_client = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'],
        token=environ['MOLGENIS_TOKEN']
    )

    organisations = ontologies_client.get(
        table='Organisations', 
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'],
        as_df=True)
    
    new_organisations = []
    for owner in owners: 
        new_row = {'name': owner}
        new_organisations.append(new_row)

    new_organisations_df = pd.DataFrame(new_organisations)

    organisations = pd.concat((organisations, new_organisations_df))

    # try to upload the organisations 
    try:
        ontologies_client.save_schema(table='Organisations', 
                                  data=organisations)
    except PyclientException: # the organisations are already added
        print('Organisations contained duplicate keys and is not uploaded.')

def map_erns_to_organisations(ngs_sequencing: pd.DataFrame):
    """Mapping GPAP ERNs to organisation in CatalogueOntologies"""
    # retrieve the ERNs mapping table 
    with Client(environ['MOLGENIS_HOST'], token=environ['MOLGENIS_TOKEN']) as client_ind:
        ontology_mappings = client_ind.get(
            table='Gpap erns',
            schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGY_MAPPINGS'],
            as_df=True
        )

    # get the ontologies client
    ontologies_client = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'],
        token=environ['MOLGENIS_TOKEN']
    )

    # retrieve the organisations from the ontologies client 
    organisations = ontologies_client.get(
        table='Organisations', 
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'],
        as_df=True)

    # retrieve the new value (the RD3 value) of the ERNs
    new_organisations = []
    gpap_erns = ngs_sequencing['erns'].unique().tolist() # gather all unique erns as a list 
    for gpap_ern in gpap_erns:
        if gpap_ern == 'Not_Applicable':
            print('NA')
            ngs_sequencing.loc[ngs_sequencing['erns'] == gpap_ern, 'erns'] = None
            print(ngs_sequencing.loc[ngs_sequencing['erns'] == gpap_ern, 'erns'])
            continue
        new_value_ern = ontology_mappings.loc[ontology_mappings['incoming value'] == gpap_ern, 'new value'].squeeze()
        new_row = {'name': new_value_ern}
        new_organisations.append(new_row)
        ngs_sequencing.loc[ngs_sequencing['erns'] == gpap_ern, 'erns'] = new_value_ern
    
    new_organisations_df = pd.DataFrame(new_organisations)

    organisations = pd.concat((organisations, new_organisations_df))

    # upload the organisations with the ERNs 
    try:
        ontologies_client.save_schema(table='Organisations', 
                                  data=organisations)
    except PyclientException: 
        print('Organisations contained duplicate keys and is not uploaded.')

    return ngs_sequencing


def build_import_NGS_sequencing(client: Client, data: pd.DataFrame):
    """This function maps GPAP experiments to NGS sequencing in RD3"""
    ngs_sequencing = data[['ExperimentID', 'LocalExperimentID', 
                           'kit', 
                           'Owner', 
                            'erns', 
                            'tissue', 'project', 'subproject', 
                           'Participant_ID',
                        #    'Submitter_ID', # TODO not sure what this is
                           'library_strategy', 
                           'Sample_ID', 'library_source']]\
    .rename(columns={
        'ExperimentID':'id',
        'LocalExperimentID': 'alternate ids',
        'kit': 'target enrichment kit',
        'tissue': 'tissue type',
        'Participant_ID': 'individuals',
        #'Submitter_ID': '',
        'Sample_ID': 'sample id',
        'library_strategy': 'library strategy',
        'library_source': 'library source'
        })

    ## map tissue type
    ontology_name = 'Tissue type'
    field_name = 'tissue type'
    matches, unmatched = asyncio.run(match_ontology(ontology=ontology_name, gpap_data=ngs_sequencing[field_name]))
    ngs_sequencing[field_name] = ngs_sequencing[field_name].replace(matches)

    unmatched_names = [entry['invalid name'] for entry in unmatched]
    tmp = ngs_sequencing.loc[ngs_sequencing[field_name].isin(
        unmatched_names)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    ngs_sequencing = ngs_sequencing.drop(tmp, axis=0)

    ## map target enrichment kit
    ontology_name = 'Sequencing enrichment kits'
    field_name = 'target enrichment kit'
    matches, unmatched = asyncio.run(match_ontology(ontology=ontology_name, gpap_data=ngs_sequencing[field_name].to_list()))
    ngs_sequencing[field_name] = ngs_sequencing[field_name].replace(matches)

    unmatched_names = [entry['invalid name'] for entry in unmatched]
    tmp = ngs_sequencing.loc[ngs_sequencing[field_name].isin(
        unmatched_names)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology equivalent
    # ngs_sequencing = ngs_sequencing.drop(tmp, axis=0)
    ngs_sequencing = ngs_sequencing.drop(columns=[field_name])

    ## map (sub)projects # TODO: this needs to be improved, is only necessary at initialization 
    # get the resources
    make_agent()
    make_endpoint()
    add_resources()
    asyncio.run(upload_curation(client=client))

    # combine project and subproject from GPAP to included in resources in RD3
    ngs_sequencing.loc[ngs_sequencing['project'].isin(['LatinSeq ERDERA']), 'project'] = 'ERDERA' # rename project LatinSeq ERDERA to ERDERA
    # tmp remove subproject for now based on GPAP's comment
    ngs_sequencing['subproject'] = None
    ngs_sequencing['included in resources'] = ngs_sequencing[['project', 'subproject']].apply(
        lambda x: ','.join(x.dropna()), axis=1
        )
    ngs_sequencing = ngs_sequencing.drop(columns=['project', 'subproject'])

    ## map library strategy
    ontology_name = 'Sequencing methods'
    field_name = 'library strategy'
    matches, unmatched = asyncio.run(match_ontology(ontology=ontology_name, gpap_data=ngs_sequencing[field_name]))
    ngs_sequencing[field_name] = ngs_sequencing[field_name].replace(matches)

    unmatched_names = [entry['invalid name'] for entry in unmatched]
    tmp = ngs_sequencing.loc[ngs_sequencing[field_name].isin(
        unmatched_names)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    ngs_sequencing = ngs_sequencing.drop(tmp, axis=0)

    ## map library source
    ontology_name = 'Library source'
    field_name = 'library source'
    matches, unmatched = asyncio.run(match_ontology(ontology=ontology_name, gpap_data=ngs_sequencing[field_name]))
    ngs_sequencing[field_name] = ngs_sequencing[field_name].replace(matches)

    unmatched_names = [entry['invalid name'] for entry in unmatched]
    tmp = ngs_sequencing.loc[ngs_sequencing[field_name].isin(
        unmatched_names)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    ngs_sequencing = ngs_sequencing.drop(tmp, axis=0)

    ## map affiliated organisations based on erns and owner columns
    owners = ngs_sequencing['Owner'].unique().tolist() # gather all unique owners as a list 
    map_owner_to_organisation(owners=owners) # upload the owners as organisations

    # erns = ngs_sequencing['erns'].unique().tolist() # gather all unique erns as a list 
    ngs_sequencing = map_erns_to_organisations(ngs_sequencing) # upload the erns as organisations

    ngs_sequencing['affiliated organisations'] = None
    for index, row in ngs_sequencing.iterrows():
        erns = row['erns']
        owner = row['Owner']
        if not pd.isna(erns):
            ngs_sequencing.loc[index, 'affiliated organisations'] = ','.join(str(field) for field in [erns, owner] if pd.notna(field))

    # remove erns and owner columns
    ngs_sequencing = ngs_sequencing.drop(columns=['erns', 'Owner']) 

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
