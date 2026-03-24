"""RD3 Staging area mapping script: mapping experiments from GPAP to RD3
"""
import logging
from os import environ
import pandas as pd
from molgenis_emx2_pyclient.client import Client
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
    
def add_resources():
    """Adding ERDERA and EMX2 API as resources to RD3. This function should be a part of a setting up script"""
    resources = pd.DataFrame({
        'id': ['ERDERA','EMX2 API'],
        'name': ['ERDERA', 'EMX2 API'],
        'type': ['Registry', 'Other type'],
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
                'publisher.resource':'EMX2 API',
                'publisher.id':'MOLGENIS',
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
        'resource': 'EMX2 API',
        'id': 'MOLGENIS',
        'website': 'https://molgenis.org/',
        'email': 'support@molgenis.org',
        'mg_draft': 'FALSE'
    }

    # save agent
    pd.DataFrame([agent]).to_csv(f'{environ['OUTPUT_PATH']}Agents.csv', index=False)

async def upload_curation(client: Client):
    """Upload Resources, Agent, and Endpoint"""
    # make a name for the zipped folder
    zip_file_name=f'{environ['OUTPUT_PATH']}archive.zip'
    # zip the data
    with ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as my_zip:
        my_zip.write(f'{environ['OUTPUT_PATH']}Agents.csv', 'Agents.csv')
        my_zip.write(f'{environ['OUTPUT_PATH']}Endpoint.csv', 'Endpoint.csv')
        my_zip.write(f'{environ['OUTPUT_PATH']}Resources.csv', 'Resources.csv')
    # upload the zipped file with the molgenis schema and the molgenis members
    await client.upload_file(schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'], file_path=zip_file_name)

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
   
    with Client(environ['MOLGENIS_HOST'], token=environ['MOLGENIS_TOKEN']) as client_ind:
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
        environ['MOLGENIS_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGY_MAPPINGS'],
        token=environ['MOLGENIS_TOKEN']
    )

    # upload the values without a match to the ontology mappings schema
    molgenis.save_schema(table=get_mappings_name(gpap_data.name)[0], data=unmatched_df)
    
    return mappings_dict, unmatched

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
    
    # get the new organisations 
    new_organisations = [owner for owner in owners if owner not in organisations['name'].to_list()]
    
    new_organisations_df = pd.DataFrame({'name': new_organisations})

    # upload the new organisations 
    ontologies_client.save_schema(table='Organisations', 
                                  data=new_organisations_df)

def build_import_NGS_sequencing(client: Client, data: pd.DataFrame):
    """This function maps GPAP experiments to NGS sequencing in RD3"""
    ngs_sequencing = data[['ExperimentID', 'LocalExperimentID', 
                           'kit', 
                           'Owner', 
                            'erns', 
                            'tissue', 'project', 'subproject', 
                           'Participant_ID',
                           'library_strategy', 
                           'Sample_ID', 'library_source']]\
    .rename(columns={
        'ExperimentID':'id',
        'LocalExperimentID': 'alternate ids',
        'kit': 'target enrichment kit',
        'tissue': 'tissue type',
        'Participant_ID': 'individuals',
        'Sample_ID': 'sample id',
        'library_strategy': 'library strategy',
        'library_source': 'library source'
        })

    ## map tissue type
    field_name = 'tissue type'
    matches, unmatched = match_ontology(gpap_data=ngs_sequencing[field_name])
    ngs_sequencing[field_name] = ngs_sequencing[field_name].replace(matches)

    tmp = ngs_sequencing.loc[ngs_sequencing[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    ngs_sequencing = ngs_sequencing.drop(tmp, axis=0)

    ## map target enrichment kit
    field_name = 'target enrichment kit'
    matches, unmatched = match_ontology(gpap_data=ngs_sequencing[field_name])

    ngs_sequencing[field_name] = ngs_sequencing[field_name].replace(matches)

    tmp = ngs_sequencing.loc[ngs_sequencing[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology equivalent
    ngs_sequencing = ngs_sequencing.drop(columns=[field_name])

    ## map (sub)projects # TODO: this needs to be improved, is only necessary at initialization 
    # get the resources
    make_agent()
    make_endpoint()
    add_resources()
    asyncio.run(upload_curation(client=client))

    # combine project and subproject from GPAP to included in resources in RD3
    ngs_sequencing.loc[ngs_sequencing['project'].isin(['LatinSeq ERDERA', 'Solve-RD ERDERA']), 'project'] = 'ERDERA' # rename project LatinSeq ERDERA to ERDERA
    # tmp remove subproject for now based on GPAP's comment
    ngs_sequencing['subproject'] = None
    ngs_sequencing['included in resources'] = ngs_sequencing[['project', 'subproject']].apply(
        lambda x: ','.join(x.dropna()), axis=1
        )
    ngs_sequencing = ngs_sequencing.drop(columns=['project', 'subproject'])

    ## map library strategy
    field_name = 'library strategy'
    matches, unmatched = match_ontology(gpap_data=ngs_sequencing[field_name])
    ngs_sequencing[field_name] = ngs_sequencing[field_name].replace(matches)
    # experiment types 

    tmp = ngs_sequencing.loc[ngs_sequencing[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    ngs_sequencing = ngs_sequencing.drop(tmp, axis=0)

    ## map library source
    field_name = 'library source'
    matches, unmatched = match_ontology(gpap_data=ngs_sequencing[field_name])
    ngs_sequencing[field_name] = ngs_sequencing[field_name].replace(matches)
    
    tmp = ngs_sequencing.loc[ngs_sequencing[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    ngs_sequencing = ngs_sequencing.drop(tmp, axis=0)

    ## map affiliated organisations based on erns and owner columns
    owners = ngs_sequencing['Owner'].unique().tolist() # gather all unique owners as a list 
    map_owner_to_organisation(owners=owners) # upload the owners as organisations

    field_name = 'erns'
    matches, unmatched = match_ontology(gpap_data=ngs_sequencing[field_name])
    ngs_sequencing[field_name] = ngs_sequencing[field_name].replace(matches)
    
    tmp = ngs_sequencing.loc[ngs_sequencing[field_name].isin(
        unmatched)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 ontology term equivalent
    ngs_sequencing = ngs_sequencing.drop(tmp, axis=0)

    ngs_sequencing['affiliated organisations'] = None
    for index, row in ngs_sequencing.iterrows():
        erns = row['erns']
        owner = row['Owner']
        if not pd.isna(erns):
            ngs_sequencing.loc[index, 'affiliated organisations'] = ','.join(str(field) for field in [erns, owner] if pd.notna(field))

    # remove erns and owner columns
    ngs_sequencing = ngs_sequencing.drop(columns=['erns', 'Owner']) 

    # to do: discuss kits 
    ngs_sequencing['target enrichment kit'] = None

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
