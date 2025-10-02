"""RD3 Staging area mapping scripts
"""
import logging
from os import environ
import pandas as pd
from molgenis_emx2_pyclient.client import Client
from dotenv import load_dotenv
import math
import ast
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
        .rename(columns={'famid': 'id'}) \
        .drop_duplicates() \
        .sort_values(by="id")
    
    # get the pedigree information with family_id (a.k.a alternate ids)
    pedigree_alternates = data[['famid', 'family_id']]
    # initialise the alternate ids column 
    pedigree['alternate ids'] = ""
    for index, family in pedigree.iterrows():
        # retrieve the family IDs for this family 
        alternate_ids = pedigree_alternates.loc[pedigree_alternates['famid'] == family['id'], 'family_id'].to_list()
        # check if there is any data (other than NA)
        if not pd.Series(alternate_ids).isna().all():
            # get the filtered data, without NAs 
            non_na = [id for id in alternate_ids if not pd.isna(id)]
            # add the alternate IDs to the pedigree data 
            pedigree.loc[index, 'alternate ids'] = ','.join(set(non_na))

    # upload
    client.save_schema(table='Pedigree', data=pedigree)

def build_import_individuals_table(client, data: pd.DataFrame):
    """Map staging area data into the Individuals table"""
    individuals = data[['id', 'sex', 'lifeStatus', 'report_date', 'last_modification_date', 'report_id']] \
    .rename(columns={
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

    # upload individuals data to RD3
    client.save_schema(table='Individuals', data=individuals)

def build_import_pedigree_members(client, data: pd.DataFrame):
    """ SKIPPED FOR NOW - needs discussion 
    
    Map staging area data into the Pedigree members table
    If index = Yes, then relative is itself (i.e., the patient). 
    If index = No, then relative is the individual of the same family with index set to yes.
    If index = No and there is not relative in the same family with index set to yes --> ?? Does this happen? 
    """
    pedigree_members = data[['report_id', 'famid', 'index', 'affectedStatus']] \
    .rename(columns={
        'report_id': 'individual',
        'famid': 'pedigree',
        'affectedStatus': 'affected'
    })

    # initialise new column to capture relative and/or relation information
    pedigree_members['relative'] = None
    pedigree_members['relation'] = None

    # gather relative and relation information based on index column
    unclear_members = []
    for index, member in pedigree_members.iterrows():
        is_index = member.get('index')
        famid = member.get('pedigree')
        if is_index == 'Yes': # if index = Yes, the individual is a patient 
            pedigree_members.loc[index, 'relative'] = member.get('individual')
            pedigree_members.loc[index, 'relation'] = 'Patient'
        elif is_index == 'No': 
            # if the individual is not the index, get the family member that is the index
            relative = pedigree_members.loc[(pedigree_members['pedigree'] == famid) & 
                                 (pedigree_members['index'] == 'Yes'), 'individual'].squeeze()
            if len(relative) != 0: 
                pedigree_members.loc[index, 'relative'] = relative
            else: # this needs to be discussed - we don't know for sure whether this is a patient or a family member 
                print(f'no diseases relative for individual: {member.get('individual')} from family: {famid}')
                unclear_members.append({'individual': member.get('individual'),
                                        'family ID': famid})
                pedigree_members.loc[index, 'relative'] = member.get('individual')
    
    # remove the index column 
    pedigree_members = pedigree_members.drop(columns={'index'})

    # map affected 
    affected_dict = {
        'Unaffected': False,
        'Affected': True
    }
    pedigree_members['affected'] = pedigree_members['affected'].map(affected_dict)

    # write pedigree information to file 
    output_path = environ['OUTPUT_PATH']
    pedigree_members.to_csv(f'{output_path}pedigree members.csv', index=False)
    pd.DataFrame(unclear_members).to_csv(f'{output_path}unclear_family_members.csv', index=False)

    # upload - skipped
    # client.save_schema(table = 'Pedigree members', data = pedigree_members)

def build_import_clinical_observations(client, data: pd.DataFrame):
    """Map staging area data into the clinical observations table"""
    clinical_observations = data[['report_id', 'solved']] \
    .rename(columns={
        'report_id': 'individuals',
        'solved': 'is solved'
    })

    # map solved field
    solved_dict = {
        'Solved': True,
        'Unsolved': False
    }
    clinical_observations['is solved'] = clinical_observations['is solved'].map(solved_dict)

    # upload
    client.save_schema(table='Clinical observations', data = clinical_observations)

def build_import_consent(client, data: pd.DataFrame):
    """Map staging area data to the Individual consent data
    TODO: field mme will need to be added (we currently do not have a field for this in RD3)"""
    indv_consent = data[['report_id']] \
    .rename(columns={
        'report_id': 'individuals'
    })
    client.save_schema(table = 'Individual consent', data = indv_consent)

def build_import_disease_history(client, data:pd.DataFrame):
    """Map staging area data to disease history data"""
    disease_history = data[['onset', 'diagnosis', 'baselineage', 'report_id']] \
    .rename(columns={
        'onset': 'age group at onset',
        'diagnosis': 'disease',
        'baselineage': 'age of onset'
    })

    # the auto IDs are necessary from clinical observations
    clinical_obs = client.get(schema = environ['MOLGENIS_HOST_SCHEMA_TARGET'], 
                              table='Clinical observations',
                              as_df = True)
    
    # capture individual's auto id 
    disease_history['part of clinical observation'] = None
    for index, disease_elem in disease_history.iterrows():
        id = clinical_obs.loc[clinical_obs['individuals'] == disease_elem['report_id'], 'id'].squeeze()
        disease_history.loc[index, 'part of clinical observation'] = id
        # get disease
        # diseases = []
        diseases_all = disease_elem.get('disease')
        if isinstance(diseases_all, str):
            # print(f'first: {diseases_all}')
            diseases_all = ast.literal_eval(diseases_all)
        if isinstance(diseases_all, (list)):
            if len(diseases_all) != 0:
                diseases = [disease.get('ordo').get('name') for disease in diseases_all if disease.get('ordo').get('name') is not None]
                # diseases.append(disease.get('name'))
                disease_history.loc[index, 'disease'] = ','.join(diseases)

    # map age group at onset
    onset_dict = {
        'HP:0011463':'Childhood onset',
        'HP:0003577': 'Congenital onset',
        'HP:0003621': 'Juvenile onset',
        'HP:0011462': 'Young adult onset',
        'HP:0003593': 'Infantile onset',
        'HP:0003623': 'Neonatal onset',
        'HP:0003584': 'Late onset',
        'HP:0003581': 'Adult onset',
        'HP:0003596': 'Middle age onset',
        'Unknown': '' # needs to be added to the ontology, TODO 
    }
    disease_history['age group at onset'] = disease_history['age group at onset'].map(onset_dict)

    # map age group 
    disease_history['age of onset'] = "P" + disease_history['age of onset'] + "Y0M0"
    # TODO: this fails --> probably bug 
    disease_history = disease_history.drop(columns={'age of onset'})

    disease_history_filtered = disease_history[disease_history['disease'].notna() & 
                                               (disease_history['disease'] != '[]') &
                                               (disease_history['disease'] != '')]

    # TODO: this fails because disease is an ontology and not an array in RD3
    client.save_schema(table = 'Disease history', data=disease_history_filtered) 

def build_import_phenotype_observations(client, data:pd.DataFrame):
    """Map staging area data to phenotype observations data"""
    phen_observations = data[['features', 'report_id']]\
    .rename(columns={
        'features':'type'
        })
    
    # the auto IDs are necessary from clinical observations
    clinical_obs = client.get(schema = environ['MOLGENIS_HOST_SCHEMA_TARGET'], 
                              table='Clinical observations',
                              as_df = True)
    
    # capture individual's auto id 
    phen_observations['part of clinical observation'] = None
    for index, pheno_obs in phen_observations.iterrows():
        id = clinical_obs.loc[clinical_obs['individuals'] == pheno_obs['report_id'], 'id'].squeeze()
        phen_observations.loc[index, 'part of clinical observation'] = id
        
        # get disease
        # diseases = []
        observations_all = pheno_obs.get('type')
        print(observations_all)
        if isinstance(observations_all, str):
            observations_all = ast.literal_eval(observations_all)
        if isinstance(observations_all, (list)):
            if len(observations_all) != 0:
                observations = [observation.get('name') for observation in observations_all if observation.get('name') is not None]
                # diseases.append(disease.get('name'))
                phen_observations.loc[index, 'type'] = ','.join(observations)

    phen_observations_filtered = phen_observations[phen_observations['type'].notna() & 
                                               (phen_observations['type'] != '[]') &
                                               (phen_observations['type'] != '')]

    # TODO: this fails because type is an ontology and not an ontology_array in RD3
    client.save_schema(table = 'Phenotype observations', data=phen_observations_filtered)


if __name__ == "__main__":

    participants = get_staging_area_participants()

    db = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'],
        token=environ['MOLGENIS_TOKEN']
    )

    output_path = environ['OUTPUT_PATH']

    # 1. Pedigree table mapping
    build_import_pedigree_table(db, participants)

    # 2. Individuals table mapping
    build_import_individuals_table(db, participants)
    
    # 3. Pegidgree Members mapping
    build_import_pedigree_members(db, participants)

    # 4. Clinical Observations mapping
    build_import_clinical_observations(db, participants)

    # 5. Individual Consent mappings
    build_import_consent(db, participants)

    # 6. Disease History mapping
    # build_import_disease_history(db, participants)

    # 7. Phenotype Observations mapping
    build_import_phenotype_observations(db, participants)
