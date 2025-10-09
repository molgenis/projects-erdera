"""RD3 Staging area mapping scripts
"""
import logging
from os import environ
import pandas as pd
from molgenis_emx2_pyclient.client import Client
from dotenv import load_dotenv
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
    TODO: discuss with Leslie and Steve
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
    """Map staging area data to the Individual consent data"""
    indv_consent = data[['report_id', 'mme']] \
    .rename(columns={
        'report_id': 'individuals',
        'mme': 'allow recontacting'
    })

    consent_dict = {
        'Yes': 'Allow use in MatchMaker',
        'No': 'No use in MatchMaker'
    }

    indv_consent['allow recontacting'] = indv_consent['allow recontacting'].map(consent_dict)

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
        # get the auto id generated for this individual
        id = clinical_obs.loc[clinical_obs['individuals'] == disease_elem['report_id'], 'id'].squeeze()
        disease_history.loc[index, 'part of clinical observation'] = id # set auto id
        # get all diseases from this individual
        diseases_all = disease_elem.get('disease')
        # there are cases when the diseases is a string of an empty list: '[]'
        if isinstance(diseases_all, str):
            diseases_all = ast.literal_eval(diseases_all) # convert to list
        if isinstance(diseases_all, (list)): # check if the input is of type list 
            if len(diseases_all) != 0: # check if the list is not empty
                # collect all diseases from this individual
                diseases = [disease.get('ordo').get('name') for disease in diseases_all if disease.get('ordo').get('name') is not None]
                disease_history.loc[index, 'disease'] = ','.join(map(str,diseases))
    
    # data is expanded to ensure that each disease is on its own row
    disease_history.loc[:,'disease'] = disease_history['disease'].str.split(',')
    disease_history = disease_history.explode('disease')

    # map the diseases to the EMX2 term
    disease_dict = {
        'Ullrich congenital muscular dystrophy': 'Congenital muscular dystrophy, Ullrich type'
    }
    disease_history['disease'] = disease_history['disease'].replace(disease_dict)

    # map age group at onset
    # TODO: save the records that do not have a ontology term match. --> in the staging area tables save this information
    # any record that does not map should be flagged --> go through it once a month or something 
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
    # TODO: this fails --> probably bug --> is a bug, issue is created
    disease_history = disease_history.drop(columns={'age of onset'})

    disease_history_filtered = disease_history[disease_history['disease'].notna() & 
                                               (disease_history['disease'] != '[]') &
                                               (disease_history['disease'] != '')]

    # upload the data
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
    pheno_observations2 = []
    for index, pheno_obs in phen_observations.iterrows():
        # get the automatically generated id for this individual
        id = clinical_obs.loc[clinical_obs['individuals'] == pheno_obs['report_id'], 'id'].squeeze()
        
        # get all observations of this individual
        observations_all = pheno_obs.get('type')
        if isinstance(observations_all, str):
            observations_all = ast.literal_eval(observations_all)
        if isinstance(observations_all, (list)):
            if len(observations_all) != 0:
                observations = [observation.get('name') for observation in observations_all if observation.get('name') is not None]
                for observation in observations:
                    new_entry = {}
                    new_entry['part of clinical observation'] = id
                    new_entry['type'] = observation
                    if id == 'JzKaxfH6si':
                        print(new_entry)
                    pheno_observations2.append(new_entry)

    phen_observations = pd.DataFrame(pheno_observations2)
    
    phen_dict = {
        'Atypical behavior': 'Behavioral abnormality',
        'Abnormal external genitalia morphology': 'Abnormal external genitalia',
        'High -  narrow palate': 'High, narrow palate',
        'Elevated serum creatine phosphokinase': 'Elevated serum creatine kinase',
        'Seizures': 'Seizure',
        'Abnormality of the cerebrum': 'Abnormal cerebral morphology',
        'Recurrent coughing spasms': 'Mild', # TODO: we miss this in the ontology (HP:0033362)
        'Dysautonomia': 'obsolete Dysautonomia',
        'Obstipation': 'Mild', # TODO: misses
        'Hypotonia': 'Muscular hypotonia',
        'Ankle contracture': 'Ankle flexion contracture',
        'Primary microcephaly': 'Congenital microcephaly',
        'Reduced collaborative play': 'Mild', # TODO: misses
        'Decreased response to growth hormone stimulation test': 'Growth hormone deficiency',
        'Abnormality of muscle morphology': 'Abnormal skeletal muscle morphology',
        'Depression': 'Depressivity',
        'Abnormal diminished volition': 'Diminished motivation',
        'Abnormality of mental function': 'Abnormality of higher mental function',
        'Abnormal pyramidal signs': 'Abnormal pyramidal sign',
        'Nasal congestion': 'Nasal obstruction',
    }
    phen_observations['type'] = phen_observations['type'].replace(phen_dict)

    # upload
    client.save_schema(table = 'Phenotype observations', data=phen_observations.drop_duplicates())


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
    build_import_disease_history(db, participants)

    # 7. Phenotype Observations mapping
    build_import_phenotype_observations(db, participants)
