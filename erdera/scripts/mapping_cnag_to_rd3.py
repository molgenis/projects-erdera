"""Mapping GPAP participants data to RD3"""
import logging
from os import environ
import pandas as pd
from molgenis_emx2_pyclient.client import Client
from dotenv import load_dotenv
import ast
import asyncio
import numpy as np
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
    
def determine_new_participants(client: Client, data: pd.DataFrame):
    """Determine which individuals are new and which should be updated - unfinished"""
    # get current RD3 information
    rd3_individuals = client.get(table='Individuals', as_df=True)
    current_ids = rd3_individuals['id']

    # add column to capture whether an individual is new or should be updated
    data['is new'] = ~data['report_id'].isin(current_ids)

    return data

def build_import_pedigree_table(client, data: pd.DataFrame):
    """Map staging area data into the Pedigree table format"""
    # retrieve current pedigrees in RD3 - unfinished
    # current_pedigrees = client.get(table='Pedigree', as_df=True) 

    # get the pedigree information with family_id (a.k.a alternate ids) and the others affacted info
    pedigree = data[['famid', 'family_id', 'otheraffected']].rename(columns={
        'famid': 'id',
        'family_id':'alternate ids',
        'otheraffected': 'others affected'
    })

    # check if family is new - unfinished
    # pedigree['is new pedigree'] = ~pedigree['id'].isin(current_pedigrees_ids)

    # for the is new pedigree true cases all mapping steps can be done immediately 
    # for the falses we need to check if fields are different 

    others_affected_dict = {
        'Yes': True,
        'No': False
    }

    pedigree['others affected'] = pedigree['others affected'].map(
        others_affected_dict)

    pedigree = pedigree.drop_duplicates()

    # set alternate ids and others affected
    for _, family in pedigree.iterrows():
        family_id = family['id']
        # retrieve the family IDs for this family
        alternate_ids = pedigree.loc[pedigree['id']
                                                == family_id, 'alternate ids'].to_list()
        # check if there is any data (other than NA)
        if not pd.Series(alternate_ids).isna().all():
            # get the filtered data, without NAs
            non_na = [id for id in alternate_ids if not pd.isna(id)]
            # add the alternate IDs to the pedigree data (as an array)
            pedigree.loc[pedigree['id'] == family_id, 'alternate ids'] = ','.join(set(non_na))

        # get list of others affected field - i.e., are there other family members affected by the disease
        others_affected = pedigree.loc[pedigree['id'] == family_id, 'others affected'].to_list()
        non_na = [x for x in others_affected if not np.isnan(x)] # only keep non NA values
        non_na_unique = set(non_na) # get the unique values
        if len(non_na_unique) == 1: # set field
            pedigree.loc[pedigree['id'] == family_id, 'others affected'] = next(iter(non_na_unique))
        # if the length is more than 1, this field for this value is both True and False, 
        # this means that for at least one family members the 'others affected' was set to True, meaning
        # that this field can be set to True for all family members 
        elif len(non_na_unique) == 2: 
            pedigree.loc[pedigree['id'] == family_id, 'others affected'] = True
        
    pedigree = pedigree.drop_duplicates()
            
    # upload
    client.save_schema(table='Pedigree', data=pedigree)

    # work in progress - unfinished
    # updated_records = pedigree.loc[pedigree['is new'] == False].copy() # get the records to be updated
    current_pedigrees = client.get(table='Pedigree', as_df=True)

    current_pedigrees_ids = current_pedigrees['id']
    pedigree['is new pedigree'] = ~pedigree['id'].isin(current_pedigrees_ids)

    merged = pedigree.merge(current_pedigrees, on='id', how='inner', suffixes = ('_new', '_old'))

    changed_mask = (merged
                    .filter(like='_new')
                    .ne(merged.filter(like='_old').values)
                    .any(axis=1)
                    )
    
    merged['changed'] = changed_mask

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
    individuals['age at enrolment'] = pd.to_numeric(
        individuals['age at enrolment'], errors='coerce')
    individuals['age at enrolment'] = "P" + \
        individuals['age at enrolment'].astype('Int64').astype('string') + "Y"

    # upload individuals data to RD3
    client.save_schema(table='Individuals', data=individuals)


def build_import_pedigree_members(client, data: pd.DataFrame):
    """ SKIPPED FOR NOW 
    There are some incomplete families. Each family should have at least an index case, currently this is not always the case. 
    This is becase (a) GPAP has not processed all cases yet or (b) submitters have supplied incomplete metadata. 
    TODO: create a new resource that captures families without an index in order to flag the incomplete families.
    Map staging area data into the Pedigree members table
    If index = Yes, then relative is itself (i.e., the patient). 
    If index = No, then relative is the individual of the same family with index set to yes.
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
        if is_index == 'Yes':  # if index = Yes, the individual is a patient
            pedigree_members.loc[index, 'relative'] = member.get('individual')
            pedigree_members.loc[index, 'relation'] = 'Patient'
        elif is_index == 'No':
            # if the individual is not the index, get the family member that is the index
            relative = pedigree_members.loc[(pedigree_members['pedigree'] == famid) &
                                            (pedigree_members['index'] == 'Yes'), 'individual'].squeeze()
            if len(relative) != 0:
                pedigree_members.loc[index, 'relative'] = relative
            else:  # this should not be the case and should be flagged as an error
                print(f'no diseased relative for individual: {member.get('individual')} from family: {famid}')
                unclear_members.append({'individual': member.get('individual'),
                                        'family ID': famid})

    # remove the index column
    pedigree_members = pedigree_members.drop(columns={'index'})

    # map affected
    affected_dict = {
        'Unaffected': False,
        'Affected': True
    }
    pedigree_members['affected'] = pedigree_members['affected'].map(
        affected_dict)

    # write pedigree information to file
    output_path = environ['OUTPUT_PATH']
    pedigree_members.to_csv(f'{output_path}pedigree members.csv', index=False)
    pd.DataFrame(unclear_members).to_csv(
        f'{output_path}unclear_family_members.csv', index=False)

    # upload - skipped
    # client.save_schema(table = 'Pedigree members', data = pedigree_members)


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
    client.save_schema(table='Individual consent', data=indv_consent)

async def match_ontologies(ontology: str, gpap_data: dict, gpap_field_name: str, mappings_name: str):
    """
    ontology = name of the ontology in RD3
    gpap_data = a dictionary of the GPAP values with the corresponding code 
    gpap_field_name = the name of the field in GPAP 
    mappings_name = the name of the table in the ontology mappings schema
    """
    molgenis = Client(
        environ['MOLGENIS_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_ONTOLOGIES'],
        token=environ['MOLGENIS_TOKEN']
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

    # capture individual's auto id
    disease_history2 = []
    diseases_dict = {}
    for _, disease_elem in disease_history.iterrows():
        # get the P-ID
        report_id = disease_elem['report_id']
        # get the auto id generated for this individual
        id = clinical_obs.loc[clinical_obs['individuals']
                              == report_id, 'id'].squeeze()
        # get the age group information
        age_group = disease_history.loc[disease_history['report_id']
                                        == report_id, 'age group at onset']
        clinical_obs.loc[clinical_obs['individuals'] ==
                         report_id, "age group at onset"] = age_group
        # get all diseases from this individual
        diseases_all = disease_elem.get('disease')
        # there are cases when the diseases is a string of an empty list: '[]'
        if isinstance(diseases_all, str):
            diseases_all = ast.literal_eval(diseases_all)  # convert to list
        if isinstance(diseases_all, (list)):  # check if the input is of type list
            if len(diseases_all) != 0:  # check if the list is not empty
                # add a new entry for each disease in the list
                for disease in diseases_all:
                    disease_ordo = disease.get('ordo')
                    if disease_ordo is not None and len(disease_ordo) != 0:
                        if disease_ordo.get('name') is None:
                            print(report_id)
                        new_entry = {}
                        new_entry['part of clinical observation'] = id
                        new_entry['disease'] = disease.get('ordo').get('name')
                        new_entry['disease status'] = disease.get(
                            'status')  # e.g., confirmed, suspected
                        new_entry['disease code'] = disease.get('ordo').get('id')
                        disease_history2.append(new_entry)
                        # save the disease names with code as prevalent in GPAP
                        diseases_dict[f'{disease.get('ordo').get('name')},{disease.get('ordo').get(
                            'id')}'] = disease.get('ordo').get('id')

    # convert list to a dataframe
    disease_history = pd.DataFrame(disease_history2)

    diseases_dict, unmatched = asyncio.run(match_ontologies('Diseases', diseases_dict, 'diagnosis', 'Diseases'))
    # make tuple of the key 
    diseases_dict = {tuple(k.split(',')): v for k, v in diseases_dict.items()}
    # map the disease based on the name + code 
    disease_history['disease'] = disease_history.apply(lambda row: diseases_dict.get((row['disease'], row['disease code']), row['disease']), axis=1)
    #disease_history['disease'] = disease_history['disease'].replace(diseases_dict)

    # get the unmatched diseases (the diseases from GPAP not present in RD3)
    # only get the name without the code
    unmatched_names = [entry['incoming value'] for entry in unmatched]
    to_delete = [tuple(item.split(',')) for item in unmatched_names]
    mask = ~disease_history.apply(lambda row: (row['disease'], row['disease code']) in to_delete, axis=1)
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
    client.save_schema(table='Disease history', data=disease_history.drop_duplicates())
    client.save_schema(table='Clinical observations', data=clinical_obs)

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

    pheno_observations2 = []
    obs_dict = {}
    for index, pheno_obs in phen_observations.iterrows():
        # get the automatically generated id for this individual
        id = clinical_obs.loc[clinical_obs['individuals']
                              == pheno_obs['report_id'], 'id'].squeeze()
        # get all observations of this individual
        observations_all = pheno_obs.get('type')
        if isinstance(observations_all, str):
            observations_all = ast.literal_eval(observations_all)
        if isinstance(observations_all, (list)):
            if len(observations_all) != 0:
                for observation in observations_all:
                    if observation.get('name') is not None:
                        new_entry = {}
                        new_entry['part of clinical observation'] = id
                        new_entry['type'] = observation.get('name')
                        # observed (GPAP) and excluded (RD3) have opposite meanings, so negate boolean
                        new_entry['excluded'] = not observation.get('observed')
                        new_entry['phenotype code'] = observation.get('id')
                        pheno_observations2.append(new_entry)
                        # save the phenotype names with code as prevalent in GPAP
                        obs_dict[(observation.get('name'),observation.get('id'))] = observation.get('id')
                        # obs_dict[observation.get('name')] = observation.get(
                        #     'id')  # only get code (without HP:)


    # convert phenotypic observations list to df
    phen_observations = pd.DataFrame(pheno_observations2)

    # map the phenotypic features from GPAP format to RD3
    phen_dict, unmatched = asyncio.run(match_ontologies('Phenotypes', obs_dict, 'features', 'Phenotypes'))
    # phen_observations['type'] = phen_observations['type'].replace(phen_dict)
    phen_observations['type'] = phen_observations.apply(lambda row: phen_dict.get((row['type'], row['phenotype code']), row['type']), axis=1)

    # get the unmatched phenotypes (the phenotypes from GPAP not present in RD3)
    # only get the name without the HP code
    unmatched_names = [entry['incoming value'] for entry in unmatched]
  
    mask = ~phen_observations.apply(lambda row: (row['type'], row['phenotype code']) in unmatched_names, axis=1)
    phen_observations = phen_observations[mask]

    phen_observations = phen_observations.drop(columns=['phenotype code'])
    
    # there are (at least) two cases where the excluded value is both true and false for an individual
    # this is not possible since it would mean that a phenotypic feature is both observed and not-observed
    # print and log this and remove from the df 
    data_entry_errors = phen_observations.groupby(['part of clinical observation', 'type'])['excluded'].transform('nunique') > 1
    phen_observations = phen_observations[~data_entry_errors]

    logging.warning(f"Error! For these observations {phen_observations[data_entry_errors]['part of clinical observation'].unique()} \
    the excluded field is both true and false for a phenotypic feature - removing the rows")

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
        environ['MOLGENIS_HOST'],
        schema=environ['MOLGENIS_HOST_SCHEMA_TARGET'],
        token=environ['MOLGENIS_TOKEN']
    )

    output_path = environ['OUTPUT_PATH']

    # adds a column to the data with the information if an individual is new or needs to be updated
    participants = determine_new_participants(client=db, data=participants)

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
