"""Mapping GPAP participants data to RD3"""
import logging
from os import environ
import ast
import asyncio

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
    with Client(environ['EMX2_HOST'], token=environ['EMX2_HOST_TOKEN']) as client_ind:
        return client_ind.get(
            table='Participants',
            schema=environ['MOLGENIS_HOST_SCHEMA_SOURCE'],
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
    resources = pd.DataFrame({
        'id': 'Incomplete families',
        'name': 'Incomplete families',
        'description': 'Capture incomplete families, these families are missing an index case'
    })

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
    client.truncate(table='Phenotype observations', schema= 'erdera')
    client.truncate(table='Disease history', schema= 'erdera')
    client.truncate(table='Clinical observations', schema='erdera')    
    
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
    client.truncate(table='Individual consent', schema='erdera')
    # then upload
    client.save_schema(table='Individual consent', data=indv_consent)

def upload_non_matches(rd3_data: set, non_matches: set, mapping: dict):
    """Upload the observations that have a mismatch between the name and/or code. """
    # create df of the non-matches for the quality control schema
    df_mismatch_obs = pd.DataFrame(list(non_matches), columns=['name', 'code'])
    # create a df of the rd3 names and codes
    rd3_data_df = pd.DataFrame(list(rd3_data), columns=['name', 'code'])
    # create a df of the non-matches merged with the rd3 names 
    df_mismatch_names = pd.merge(df_mismatch_obs, rd3_data_df, on='code', how='inner') # mismatched on name (same code)
    df_mismatch_codes = pd.merge(df_mismatch_obs, rd3_data_df, on='name', how='inner') # mismatched on code (same name)
    
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
        environ['EMX2_HOST'],
        schema=environ['MOLGENIS_HOST_QUALITY_CONTROL'],
        token=environ['EMX2_HOST_TOKEN']
    )

    # upload the mismatched phenotypes
    molgenis.save_schema(data=quality_control_upload, table='Phenotypes')

def match_phenotypes(gpap_data: set):
    """Match phenotypes"""
    molgenis = Client(
        environ['EMX2_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'],
        token=environ['EMX2_HOST_TOKEN']
    )
    # get the RD3 ontology
    rd3_phenotypes = molgenis.get(
        table='Phenotypes', schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'], as_df=True)

    # create a set of the rd3 names and codes
    rd3_data = set(zip(rd3_phenotypes['name'], rd3_phenotypes['code']))
    # get the GPAP phenotypes that do not have a name and code match in RD3 
    non_matches = gpap_data - rd3_data

    # check for which gpap cases quality control has taken place
    molgenis = Client(
        environ['EMX2_HOST'],
        schema=environ['MOLGENIS_HOST_QUALITY_CONTROL'],
        token=environ['EMX2_HOST_TOKEN']
    )
    # get the phenotypes quality control information
    phenotypes = molgenis.get(table='Phenotypes', as_df=True)
    phenotypes_new_value = phenotypes[~phenotypes['correct phenotype'].isna()] # get the rows that have a correction

    ## case 1: there is a _correct phenotype_ entry, meaning the GPAP entry should be this phenotype
    # create a dictionary of the GPAP name and code with the new value (the correct phenotype)
    mapping = phenotypes_new_value.set_index(['GPAP name', 'GPAP code'])['correct phenotype'].to_dict()

    ## case 2: the _is correct_ boolean is set to True, meaning the RD3 variant of the GPAP phenotype is correct
    phenotypes_is_correct = phenotypes[phenotypes['is correct']]
    # for these cases, the RD3 name is the correct one
    mapping.update(phenotypes_is_correct.set_index(['GPAP name', 'GPAP code'])['RD3 name'].to_dict())

    # upload the mismatches
    upload_non_matches(rd3_data=rd3_data, non_matches=non_matches, mapping=mapping)
    # upload the cases where there is no RD3 data
    check_no_match(rd3_data=rd3_data, non_matches = non_matches)

    return non_matches, mapping

def check_no_match(rd3_data: set, non_matches: set):
    """Check if there is no RD3 match for the data entry"""
    missing_codes = set([i[1] for i in non_matches]) - set([i[1] for i in rd3_data])

    # create a dictionary of the non-matches with the codes as keys
    non_matches_dict = {y: x for x,y in non_matches}

    # create a df of the missing phenotypes (missing in RD3)
    missing_df = pd.DataFrame({
        'source': 'phenostore_service/api/participants_by_exp/features',
        'incoming value': non_matches_dict.get(code),
        'incoming code': code
    }
    for code in missing_codes)

    molgenis = Client(
        environ['EMX2_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGY_MAPPINGS'],
        token=environ['EMX2_HOST_TOKEN']
    )

    molgenis.save_schema(data=missing_df, table='Phenotypes')

async def match_ontologies(ontology: str, gpap_data: dict, gpap_field_name: str, mappings_name: str):
    """
    ontology = name of the ontology in RD3
    gpap_data = a dictionary of the GPAP values with the corresponding code 
    gpap_field_name = the name of the field in GPAP 
    mappings_name = the name of the table in the ontology mappings schema
    """
    molgenis = Client(
        environ['EMX2_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'],
        token=environ['EMX2_HOST_TOKEN']
    )
    # get the RD3 ontology and overrides
    rd3_ontology = molgenis.get(
        table=ontology, schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'], as_df=True)

    overrides = molgenis.get(
        table=mappings_name,
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGY_MAPPINGS'],
        as_df=True
    )

    # create dictionary of RD3 code and ontology name
    code_to_name = dict(zip(rd3_ontology['code'], rd3_ontology['name']))

    # save the matches (the once without a perfect name match)
    match_dict = {}
    # and save the ones that do not have a name and/or code match
    unmatched = []
    no_correct_name = []
    for ontology_value, code in gpap_data.items():
        code_no_name = code.split(':')[1]
        # in RD3 we include the codesystem in the code for the phenotypes
        if ontology == 'Phenotypes': # TODO: this should be solved some better/nicer way
            code_no_name = code
        ontology_value_no_code = ontology_value[0]
        # Case 1: name match ignore (we only want mismatched names)
        if ontology_value_no_code in rd3_ontology['name'].values:
            continue
        # Case 2: code match save to match_dict
        elif code_no_name in code_to_name:
            match_dict[ontology_value] = code_to_name[code_no_name]
        # Case 3: if the code is not known and has an override
        elif code in overrides['incoming code'].values:
            correct_name = overrides.loc[overrides['incoming code']
                                                  == code]['new value']
            if pd.notna(correct_name.iloc[0]): # check if there is an overrride
                match_dict[ontology_value] = correct_name.squeeze()
            else:
                no_correct_name.append({
                    'source': f'phenostore_service/api/participants_by_exp/{gpap_field_name}',
                    'incoming value': ontology_value,
                    'incoming code': code
                })
        # Case 4: no match at all
        else:
            unmatched.append({
                'source': f'phenostore_service/api/participants_by_exp/{gpap_field_name}',
                'incoming value': ontology_value,
                'incoming code': code
            })

    # upload the new unmatched ontology values
    output_path = environ['OUTPUT_PATH']
    pd.DataFrame(unmatched).drop_duplicates().to_csv(f'{output_path}{mappings_name}.csv', index=False)
    await molgenis.upload_file(file_path=f'{output_path}{mappings_name}.csv', schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGY_MAPPINGS'])

    # combine the new unmatched values with the already known unmatched values (but without a correct name)
    # -- these will be removed from the dataset to be uplaoded
    unmatched = unmatched + no_correct_name

    return match_dict, unmatched

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
        diseases_all = parse_observations(disease_elem.get('disease'))

        for disease in diseases_all:
            disease_ordo = disease.get('ordo')
            if not disease_ordo:
                continue

            name = disease_ordo.get('name')
            code = disease_ordo.get('id')
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

    diseases_dict, unmatched = asyncio.run(match_ontologies('Diseases', diseases_dict, 'diagnosis', 'Diseases'))
    # make tuple of the key 
    #diseases_dict = {tuple(k.split(',')): v for k, v in diseases_dict.items()}
    # map the disease based on the name + code 
    disease_history['disease'] = disease_history.apply(lambda row: diseases_dict.get((row['disease'], row['disease code']), row['disease']), axis=1)
    #disease_history['disease'] = disease_history['disease'].replace(diseases_dict)

    # get the unmatched diseases (the diseases from GPAP not present in RD3)
    # only get the name without the code
    unmatched_names = [entry['incoming value'] for entry in unmatched]
   # to_delete = [tuple(item.split(',')) for item in unmatched_names]
    mask = ~disease_history.apply(lambda row: (row['disease'], row['disease code']) in unmatched_names, axis=1)
    disease_history = disease_history[mask]
    # tmp = disease_history.loc[disease_history['disease'].isin(
    #     unmatched_names)].index  # get the indices of the rows to remove (no match)
    # # remove rows without a RD3 disease equivalent
    # disease_history = disease_history.drop(tmp, axis=0)

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
    client.truncate(table='Clinical observations', schema='erdera') # first truncate in order to update (to prevent duplicates)
    client.save_schema(table='Clinical observations', data=clinical_obs)
    client.save_schema(table='Disease history', data=disease_history.drop_duplicates())

def parse_observations(obs):
    """
    Parse the GPAP observations (features) and convert to a list.
    Works for the following cases:
    - empty values (None, NaN) → []
    - string representation of a list → []
    - list → []
    """
    # None of np.nan check
    if obs is None or (isinstance(obs, float) and np.isnan(obs)):
        return []

    # if the observation is a string
    if isinstance(obs, str):
        try:
            parsed = ast.literal_eval(obs)
            if isinstance(parsed, list):
                return parsed
            else:
                return []
        except Exception:
            return []

    # Numpy array → Python list
    if isinstance(obs, np.ndarray):
        return obs.tolist()

    # if it is already a list, return the original observation
    if isinstance(obs, list):
        return obs
    
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
        observations_all = parse_observations(pheno_obs.get('type'))
        
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
        logging.warning(f"Error! For these observations {phen_observations[data_entry_errors]['part of clinical observation'].unique()} \
        the excluded field is both true and false for the following phenotypic feature(s): \
        {phen_observations[data_entry_errors]['type'].unique().tolist()} - removing the row(s)")

        phen_observations = phen_observations[~data_entry_errors]

    # drop unneccessary columns
    phen_observations = phen_observations.drop(columns=['phenotype code', 'key'])

    # upload
    client.save_schema(table='Phenotype observations',
                       data=phen_observations.drop_duplicates())

    
def build_import_variants(client: Client, data: pd.DataFrame):
    """Builds and import variant information from GPAP to RD3
    # disabled for now -- unfinished"""

    variant_interpretations = data[[
        'inheritance'
    ]]

    # TODO: use mappings schema
    inheritance_dict = {
        'HP:0001450': 'Y-linked inheritance',
        'HP:0003745': '', # is sporadic inheritance, not included in our ontology
        'HP:0000006': 'autosomal dominant inheritance',
        'HP:0001419': 'X-linked recessive inheritance',
        'HP:0001427': 'mitochondrial inheritance',
        'HP:0001444': '', # 'typified by somatic mosaicism' not in included in our ontology
        'HP:0000007': 'autosomal recessive inheritance',
        'HP:0001470': 'sex-limited autosomal recessive inheritance, sex-limited autosomal dominant inheritance', #? the HP code is 'Sex-limited expression'
        'HP:0001417': 'X-linked inheritance'
    }

    for index, interpretation in variant_interpretations.iterrows():
        # get all observations of this individual
        inheritance_all = interpretation.get('inheritance')
        if isinstance(inheritance_all, str):
            inheritance_all = ast.literal_eval(inheritance_all)
        if isinstance(inheritance_all, (list)):
            if len(inheritance_all) != 0:
                inheritance_list = []
                for inheritance_entry in inheritance_all:
                    rd3_inheritance = inheritance_dict.get(inheritance_entry)
                    inheritance_list.append(rd3_inheritance)
        variant_interpretations.loc[index, 'inheritance'] = ','.join(inheritance_list)

    client.save_schema(table='Variant interpretations', data=variant_interpretations)

def build_import_genomic_variants(client: Client, data: pd.DataFrame):
    """Builds and imports variant information from GPAP to RD3
    Unfinished - disabled"""
    genomic_variant = data[['regions']].copy() 

    for index, region in genomic_variant.iterrows():
        region_entry = region.get('regions')
        if isinstance(region_entry, str):
            region_entry = ast.literal_eval(region_entry)
        if isinstance(region_entry, list):
            for elem in region_entry:
                genomic_variant.loc[index, 'id'] = elem.get('region')
                genomic_variant.loc[index, 'chromosomal region'] = elem.get('region')

if __name__ == "__main__":

    participants = get_staging_area_participants()

    db = Client(
        environ['EMX2_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'],
        token=environ['EMX2_HOST_TOKEN']
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
