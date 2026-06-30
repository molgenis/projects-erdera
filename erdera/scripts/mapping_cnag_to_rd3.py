"""Mapping GPAP participants data to RD3"""
import logging
from os import environ
import ast

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from molgenis_emx2_pyclient.client import Client

load_dotenv()

logging.captureWarnings(True)
log = logging.getLogger("Staging Area Mapping")

def get_staging_area_participants():
    """Retrieve metadata from /<staging area>/Participants"""
    logging.info('Retrieving required metadata')
    with Client(environ['MOLGENIS_HOST'], token=environ['MOLGENIS_TOKEN']) as client_ind:
        return client_ind.get(
            table='Participants',
            schema=environ['SCHEMA_GPAP_SOURCE'],
            as_df=True
        )
    
def build_import_pedigree_table(client, data: pd.DataFrame):
    """Map staging area data into the Pedigree table format"""
    # retrieve current pedigrees in RD3 - unfinished
    # current_pedigrees = client.get(table='Pedigree', as_df=True) 

    # get the pedigree information with family_id (a.k.a alternate ids) and the others affacted info
    pedigree = (data[['famid', 'family_id', 'otheraffected']].rename(columns={
        'famid': 'id',
        'family_id':'alternate ids',
        'otheraffected': 'others affected'
    })
    .dropna(subset = ['id'])
    )

    others_affected_dict = {
        'Yes': True,
        'No': False
    }
    pedigree['others affected'] = pedigree['others affected'].map(
        others_affected_dict)

    # gather and set the alternate IDs of the families
    alt_ids = (
        pedigree
        .dropna(subset=['alternate ids'])
        .groupby('id')['alternate ids']
        .agg(lambda x: ','.join(sorted(set(x))))
        )

    # gather and set the 'others affected' field. 
    others = (
        pedigree
        .dropna(subset=['others affected'])
        .groupby('id')['others affected']
        # if a True is present for the family, set the field of the whole family to True
        .any()
        )
    
    # merge the to DFs together
    pedigree = (
        pedigree[['id']]
        .drop_duplicates()
        .merge(alt_ids, on='id', how='left')
        .merge(others, on='id', how='left')
        )
            
    # upload
    client.save_schema(table='Pedigree', data=pedigree)

def build_import_individuals_table(client, data: pd.DataFrame):
    """Map staging area data into the Individuals table"""
    individuals = data[['id', 'sex', 'lifeStatus', 'report_date', 'last_modification_date', 'report_id', 'baselineage']] \
        .rename(columns={
            'id': 'alternate ids',
            'sex': 'gender at birth',
            'lifeStatus': 'individual status',
            'report_date': 'date created at source',
            'last_modification_date': 'date updated at source',
            'report_id': 'id',
            'baselineage': 'age at enrolment'
        })

    # TODO: retrieve this from mappings schema
    individual_status_dict = {
        'Deceased': 'Dead'
    }
    individuals['individual status'] = individuals['individual status'].replace(
        individual_status_dict)

    # TODO: retrieve this from mappings schema
    gender_dict = {
        'M': 'assigned male at birth',
        'F': 'assigned female at birth'
    }
    individuals['gender at birth'] = individuals['gender at birth'].map(
        gender_dict)

    # map age group
    age = pd.to_numeric(individuals['age at enrolment'], errors='coerce')
    individuals['age at enrolment'] = "P" + age.astype('Int64').astype('string') + "Y"

    # upload individuals data to RD3
    client.save_schema(table='Individuals', data=individuals)

def add_incomplete_families_resource(client: Client):
    """Create a new resource to capture the incomplete families"""
    resources = pd.DataFrame([{
        'id': 'Incomplete families',
        'name': 'Incomplete families',
        'description': 'Capture incomplete families, these families are missing an index case'
    }])

    # save resources
    client.save_schema(table='Resources', data=resources)

def build_import_pedigree_members(client: Client, data: pd.DataFrame):
    """ Map staging area data into the Pedigree members table
    If index = Yes, then relative is itself (i.e., the patient). 
    If index = No, then relative is the individual of the same family with index set to yes.
    """
    pedigree_members = data[['report_id', 'famid', 'index', 'affectedStatus']] \
        .rename(columns={
            'report_id': 'individual',
            'famid': 'pedigree',
            'affectedStatus': 'affected'
        })

    # remove the rows with an empty (NA) value for the family ID
    pedigree_members = pedigree_members.dropna(subset=['pedigree'])

    # create a lookup table where each family is mapped to the index individual of the family
    index_map = (pedigree_members[pedigree_members['index'] == 'Yes']
                 .set_index('pedigree')['individual'])
    # set the relative of each member to the index case of the family
    pedigree_members['relative'] = pedigree_members['pedigree'].map(index_map)
    
    # Set relation column
    pedigree_members['relation'] = None
    pedigree_members.loc[pedigree_members['index'] == 'Yes', 'relation'] = 'Patient'

    # Find families without an index
    families_wo_index = pedigree_members.loc[pedigree_members['relative'].isna(),'pedigree'].unique().tolist()
    log.info(f'The following families are incomplete, i.e., missing an index case: {families_wo_index}')
    # remove these members
    pedigree_members = pedigree_members[~pedigree_members['pedigree'].isin(families_wo_index)]

    # flag the incomplete families in the Pedigree table 
    add_incomplete_families_resource(client=client)
    pedigree = client.get('Pedigree', as_df = True)
    pedigree.loc[pedigree['id'].isin(families_wo_index),
                 'included in resources'] = 'Incomplete families'
    client.save_schema(table='Pedigree', data=pedigree) # upload with updated field 

    # remove the index column
    pedigree_members = pedigree_members.drop(columns={'index'})

    # map affected
    affected_dict = {
        'Unaffected': False,
        'Affected': True
    }
    pedigree_members['affected'] = pedigree_members['affected'].map(
        affected_dict)

    # upload
    client.save_schema(table = 'Pedigree members', data = pedigree_members)

def build_import_clinical_observations(client, data: pd.DataFrame):
    """Map staging area data into the clinical observations table"""
    clinical_observations = data[['report_id', 'solved', 'consanguinity']] \
        .rename(columns={
            'report_id': 'individuals',
            'solved': 'is solved'
        })

    # map solved field
    solved_dict = {
        'Solved': True,
        'Unsolved': False
    }
    clinical_observations['is solved'] = clinical_observations['is solved'].map(
        solved_dict)

    # map consanguinity
    consanguinity_dict = {
        'Yes': True,
        'No': False
    }
    clinical_observations['consanguinity'] = clinical_observations['consanguinity'].map(
        consanguinity_dict)

    # upload
    # first delete content of clinical observations
    client.truncate(table='Phenotype observations', schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'])
    client.truncate(table='Disease history', schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'])
    client.truncate(table='Clinical observations', schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'])    
    
    # then upload
    client.save_schema(table='Clinical observations',
                       data=clinical_observations)


def build_import_consent(client, data: pd.DataFrame):
    """Map staging area data to the Individual consent data"""
    indv_consent = data[['report_id', 'mme']] \
        .rename(columns={
            'report_id': 'individuals',
            'mme': 'allow recontacting'
        })

    # map matchmaker consent
    consent_dict = {
        'Yes': 'Allow use in MatchMaker',
        'No': 'No use in MatchMaker'
    }
    indv_consent['allow recontacting'] = indv_consent['allow recontacting'].map(
        consent_dict)

    # upload the data
    # first truncate the consent table
    client.truncate(table='Individual consent', schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'])
    # then upload
    client.save_schema(table='Individual consent', data=indv_consent)

def upload_non_matches(rd3_data: set, non_matches: set, mapping: dict, rd3_ontology_name: str):
    """Upload the entries that have a mismatch between the name and/or code. """
    # create df of the non-matches for the quality control schema
    df_mismatches = pd.DataFrame(list(non_matches), columns=['name', 'code'])
    # create a df of the rd3 names and codes
    rd3_data_df = pd.DataFrame(list(rd3_data), columns=['name', 'code'])
    # create a df of the non-matches merged with the rd3 names 
    df_mismatch_names = pd.merge(df_mismatches, rd3_data_df, on='code', how='inner') # mismatched on name (same code)
    df_mismatch_codes = pd.merge(df_mismatches, rd3_data_df, on='name', how='inner') # mismatched on code (same name)
    
    df_mismatch_names = df_mismatch_names.rename(columns= { # rename columns to correspond to schema
        'name_x': 'GPAP name',
        'code': 'GPAP code',
        'name_y': 'RD3 name'
    })
    df_mismatch_names['RD3 code'] = df_mismatch_names['GPAP code'] # the codes are identical between GPAP and RD3
    df_mismatch_names['type of mismatch'] = 'name' # the type of mismatch is on the name 

    df_mismatch_codes = df_mismatch_codes.rename(columns= { # rename columns to correspond to schema
        'name': 'GPAP name',
        'code_x': 'GPAP code',
        'code_y': 'RD3 code'
    })
    df_mismatch_codes['RD3 name'] = df_mismatch_codes['GPAP name'] # the names are identical between GPAP and RD3
    df_mismatch_codes['type of mismatch'] = 'code' # the type of mismatch is on the code

    # upload the non-matches minus the mappings (for the mappings there is a correction)
    quality_control_upload = pd.concat([df_mismatch_codes, df_mismatch_names])
    quality_control_upload = quality_control_upload[~quality_control_upload[['GPAP name', 'GPAP code']] \
    .apply(tuple, axis=1).isin(set(mapping))]

    molgenis = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['SCHEMA_QUALITY_CONTROL'],
        token=environ['MOLGENIS_TOKEN']
    )

    # upload the mismatches
    molgenis.save_schema(data=quality_control_upload, table=rd3_ontology_name)

def check_no_match(rd3_data: set, non_matches: set, rd3_ontology_name: str, mapping: dict):
    """Check if there is no RD3 match for the data entry"""
    # get the codes of the GPAP entries that do not have an RD3 match (i.e., this code is missing in the RD3 ontology)
    missing_codes = set([i[1] for i in non_matches]) - set([i[1] for i in rd3_data])

    # create a dictionary of the non-matches with the codes as keys
    non_matches_dict = {y: x for x,y in non_matches}

    # create a df of the missing entries (missing in RD3)
    missing_df = pd.DataFrame({
        'source': 'phenostore_service/api/participants_by_exp/features',
        'incoming value': non_matches_dict.get(code),
        'incoming code': code
    }
    for code in missing_codes)

    molgenis = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['SCHEMA_ONTOLOGY_MAPPINGS'],
        token=environ['MOLGENIS_TOKEN']
    )
    # check if there are new values in the ontology mappings schema to prevent overwrite during upload 
    ontology_mappings_data = molgenis.get(table=rd3_ontology_name, as_df=True)
    new_value = ontology_mappings_data[~ontology_mappings_data['new value'].isna()]
    # update the mapping dictionary with the new value 
    mapping.update(new_value.set_index(['incoming value', 'incoming code'])['new value'].to_dict())

    # make sure the value(s) that have a new value are removed from the df, so the new value will not be overwritten
    missing_df = (missing_df.merge(new_value[['source', 'incoming value', 'incoming code']].drop_duplicates(),
                  on=['source', 'incoming value', 'incoming code'],
                  how='left',
                  indicator=True)
                  .query("_merge == 'left_only'")
                  .drop(columns='_merge'))

    # save the df
    molgenis.save_schema(data=missing_df, table=rd3_ontology_name)

def match_phenotypes(gpap_data: set):
    """Wrapper function to match the phenotypes"""
    return match_ontologies(gpap_data=gpap_data, rd3_ontology_name = 'Phenotypes', qc_correct = 'correct phenotype')

def match_diseases(gpap_data: set):
    """Wrapper function to match the diseases"""
    return match_ontologies(gpap_data=gpap_data, rd3_ontology_name = 'Diseases', qc_correct = 'correct disease')

def match_ontologies(gpap_data: set, rd3_ontology_name: str, qc_correct: str):
    """Match GPAP ontologies to RD3 and find mismatches"""
    molgenis = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['SCHEMA_ONTOLOGIES'],
        token=environ['MOLGENIS_TOKEN']
    )
    # get the RD3 ontology
    rd3_ontology = molgenis.get(
        table=rd3_ontology_name, schema=environ['SCHEMA_ONTOLOGIES'], as_df=True)

    # create a set of the rd3 names and codes
    rd3_data = set(zip(rd3_ontology['name'], rd3_ontology['code']))
    # get the GPAP ontology values that do not have a name and code match in RD3 
    non_matches = gpap_data - rd3_data

    # check for which gpap cases quality control has taken place
    molgenis = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['SCHEMA_QUALITY_CONTROL'],
        token=environ['MOLGENIS_TOKEN']
    )
    # get the quality control information
    qc_info = molgenis.get(table=rd3_ontology_name, as_df=True)
    new_value = qc_info[~qc_info[qc_correct].isna()] # get the rows that have a correction

    ## case 1: there is a new entry, meaning the GPAP entry should be that
    # create a dictionary of the GPAP name and code with the new value (the correct value)
    mapping = new_value.set_index(['GPAP name', 'GPAP code'])[qc_correct].to_dict()

    ## case 2: the _is correct_ boolean is set to True, meaning the RD3 variant of the GPAP entry is correct
    is_correct = qc_info[qc_info['is correct']]
    # for these cases, the RD3 name is the correct one
    mapping.update(is_correct.set_index(['GPAP name', 'GPAP code'])['RD3 name'].to_dict())

    # upload the mismatches
    upload_non_matches(rd3_data=rd3_data, 
                       non_matches=non_matches, 
                       mapping=mapping, 
                       rd3_ontology_name=rd3_ontology_name)
    # upload the cases where there is no RD3 data
    check_no_match(rd3_data=rd3_data, 
                   non_matches = non_matches,
                   rd3_ontology_name=rd3_ontology_name,
                   mapping=mapping)

    # remove the entries that have a mapping from the non-matches list
    non_matches = non_matches - set(mapping.keys())

    return non_matches, mapping

def build_import_disease_history(client, data: pd.DataFrame):
    """Map staging area data to disease history data"""
    disease_history = data[['onset', 'diagnosis', 'report_id']] \
        .rename(columns={
            'onset': 'age group at onset',
            'diagnosis': 'disease'
        })

    # the auto IDs are necessary from clinical observations
    clinical_obs = client.get(schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'],
                              table='Clinical observations',
                              as_df=True)
    # create a map of individual ID and the corresponding auto generated ID
    id_map = clinical_obs.set_index('individuals')['id']

    disease_history2 = []
    diseases_set = set()
    for _, disease_elem in disease_history.iterrows():
        # get the P-ID
        report_id = disease_elem['report_id']
        # get the auto id generated for this individual
        id = id_map.get(report_id)

        # get the age group information
        age_group = disease_history.loc[disease_history['report_id']
                                        == report_id, 'age group at onset']
        clinical_obs.loc[clinical_obs['individuals'] ==
                         report_id, "age group at onset"] = age_group
        
        # get all diseases from this individual
        diseases_all = parse_entries(disease_elem.get('disease'))

        for disease in diseases_all:
            disease_ordo = disease.get('ordo')
            if not disease_ordo:
                continue

            name = disease_ordo.get('name')
            code = disease_ordo.get('id').split(':')[1].strip() # remove prefix and whitespace
            status = disease.get('status')

            if name is None:
                print(report_id)

            disease_history2.append({
                'part of clinical observation': id,
                'disease': name,
                'disease status': status,  # e.g. confirmed, suspected
                'disease code': code
            })

            diseases_set.add((name, code))

    # convert list to a dataframe
    disease_history = pd.DataFrame(disease_history2)

    non_matches, mappings = match_diseases(diseases_set)

    disease_history['key'] = list(zip(disease_history['disease'], disease_history['disease code']))

    # map the corrections
    disease_history.loc[disease_history['key'].isin(mappings), 'disease'] = disease_history['key'].map(mappings)
    # remove the non-matches
    # first remake the key so the corrections are incorporated
    disease_history['key'] = list(zip(disease_history['disease'], disease_history['disease code']))
    disease_history = disease_history[~disease_history['key'].isin(non_matches)]
   
    # map the disease status TODO: use the mappings schema
    status_dict = {
        'Confirmed': 'Confirmed diagnosis'
    }
    disease_history['disease status'] = disease_history['disease status'].replace(
        status_dict)

    # map age group at onset
    # TODO: save the records that do not have a ontology term match. --> in the staging area tables save this information
    # any record that does not map should be flagged --> go through it once a month or something
    # TODO: use the mappings schema
    onset_dict = {
        'HP:0011463': 'Childhood onset',
        'HP:0003577': 'Congenital onset',
        'HP:0003621': 'Juvenile onset',
        'HP:0011462': 'Young adult onset',
        'HP:0003593': 'Infantile onset',
        'HP:0003623': 'Neonatal onset',
        'HP:0003584': 'Late onset',
        'HP:0003581': 'Adult onset',
        'HP:0003596': 'Middle age onset',
        'Unknown': ''  # needs to be added to the ontology, TODO
    }
    clinical_obs['age group at onset'] = clinical_obs['age group at onset'].map(
        onset_dict)
    
    # remove the disease code field 
    disease_history = disease_history.drop(columns=['disease code'])

    # upload the data
    client.truncate(table='Clinical observations', schema=environ['MOLGENIS_HOST_SCHEMA_TARGET']) # first truncate in order to update (to prevent duplicates)
    client.save_schema(table='Clinical observations', data=clinical_obs)
    client.save_schema(table='Disease history', data=disease_history.drop_duplicates())

def parse_entries(entries):
    """
    Parse the GPAP entries (phenotypes and diseases) and convert to a list.
    Works for the following cases:
    - empty values (None, NaN) → []
    - string representation of a list → []
    - list → []
    """
    # None of np.nan check
    if entries is None or (isinstance(entries, float) and np.isnan(entries)):
        return []

    # if the entry is a string
    if isinstance(entries, str):
        try:
            parsed = ast.literal_eval(entries)
            if isinstance(parsed, list):
                return parsed
            else:
                return []
        except Exception:
            return []

    # Numpy array → Python list
    if isinstance(entries, np.ndarray):
        return entries.tolist()

    # if it is already a list, return the original observation
    if isinstance(entries, list):
        return entries
    
    # in all other cases,  return list 
    return []

def build_import_phenotype_observations(client, data: pd.DataFrame):
    """Map staging area data to phenotype observations data"""
    phen_observations = data[['features', 'report_id']]\
        .rename(columns={
            'features': 'type'
        })

    # the auto IDs are necessary from clinical observations
    clinical_obs = client.get(schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'],
                              table='Clinical observations',
                              as_df=True)
    # create a map of individual ID and the corresponding auto generated ID
    id_map = clinical_obs.set_index('individuals')['id']

    pheno_observations2 = []
    observations = set()
    for _, pheno_obs in phen_observations.iterrows():
        # get the automatically generated id for this individual
        report_id = pheno_obs['report_id']
        id = id_map.get(report_id)

        # check if the individual has a clinical observations ID - otherwise the individual is present in the 
        # GPAP staging area data but not in RD3
        if pd.isna(id):
            logging.warning(f'Individual {pheno_obs['report_id']} does not have a clinical observation ID. The \
    individual is not included in the Phenotype Observations.')
            continue
        
        # get all observations of this individual
        observations_all = parse_entries(pheno_obs.get('type'))
        
        # loop through the phenotypic observations of the individual
        for observation in observations_all:
            name = observation.get('name')
            if name is None:
                continue
            
            pheno_observations2.append({
                'part of clinical observation': id,
                'type': name,
                'excluded': not observation.get('observed'),
                'phenotype code': observation.get('id')
            })
            # save the phenotype names with code as prevalent in GPAP
            observations.add((name, observation.get('id')))

    # convert phenotypic observations list to df
    phen_observations = pd.DataFrame(pheno_observations2)

    # map the phenotypic features from GPAP format to RD3
    non_matches, mappings = match_phenotypes(observations)

    phen_observations['key'] = list(zip(phen_observations['type'], phen_observations['phenotype code']))

    # map the corrections
    phen_observations.loc[phen_observations['key'].isin(mappings), 'type'] = phen_observations['key'].map(mappings)
    # remove the non-matches
    # first remake the key so the corrections are incorporated 
    phen_observations['key'] = list(zip(phen_observations['type'], phen_observations['phenotype code']))
    phen_observations = phen_observations[~phen_observations['key'].isin(non_matches)]
    
    # there are (at least) two cases where the excluded value is both true and false for an individual
    # this is not possible since it would mean that a phenotypic feature is both observed and not-observed
    # print and log this and remove from the df 
    data_entry_errors = phen_observations.groupby(['part of clinical observation', 'type'])['excluded'].transform('nunique') > 1

    if not phen_observations[data_entry_errors].empty:
        logging.warning(f"Warning! For these observations {phen_observations[data_entry_errors]['part of clinical observation'].unique()} \
        the excluded field is both true and false for the following phenotypic feature(s): \
        {phen_observations[data_entry_errors]['type'].unique().tolist()} - removing the row(s)")

        phen_observations = phen_observations[~data_entry_errors]

    # drop unneccessary columns
    phen_observations = phen_observations.drop(columns=['phenotype code', 'key'])

    # upload
    client.save_schema(table='Phenotype observations',
                       data=phen_observations.drop_duplicates())

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