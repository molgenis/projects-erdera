"""RD3 Staging area mapping scripts
"""
import logging
from os import environ
import pandas as pd
from molgenis_emx2_pyclient.client import Client
from dotenv import load_dotenv
import ast
import asyncio
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
    """Determine which individuals are new and which should be updated"""
    # get current RD3 information
    rd3_individuals = client.get(table='Individuals', as_df=True)
    current_ids = rd3_individuals['id']

    # add column to capture whether an individual is new or should be updated
    data['is new'] = ~data['report_id'].isin(current_ids)

    return data

def build_import_pedigree_table(client, data: pd.DataFrame):
    """Map staging area data into the Pedigree table format"""
    # retrieve current pedigrees in RD3
    # current_pedigrees = client.get(table='Pedigree', as_df=True) 

    # get the pedigree information with family_id (a.k.a alternate ids) and the others affacted info
    # pedigree = data[['famid', 'family_id', 'otheraffected']].rename(columns={
    #     'famid': 'id',
    #     'family_id':'alternate ids',
    #     'otheraffected': 'others affected'
    # })

    pedigree = data[['famid', 'family_id']].rename(columns={
        'famid': 'id',
        'family_id':'alternate ids'
        })

    # check if family is new 
    # pedigree['is new pedigree'] = ~pedigree['id'].isin(current_pedigrees_ids)

    # for the is new pedigree true cases all mapping steps can be done immediately 
    # for the falses we need to check if fields are different 
    

    others_affected_dict = {
        'Yes': True,
        'No': False
    }

    # pedigree['others affected'] = pedigree['others affected'].map(
    #     others_affected_dict)

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
        # others_affected = pedigree.loc[pedigree['id'] == family_id, 'others affected'].to_list()
        # non_na = [x for x in others_affected if not np.isnan(x)] # only keep non NA values
        # non_na_unique = set(non_na) # get the unique values
        # if len(non_na_unique) == 1: # set field
        #     pedigree.loc[pedigree['id'] == family_id, 'others affected'] = next(iter(non_na_unique))
        # # if the length is more than 1, this field for this value is both True and False, 
        # # this cannot be the case so print a message to the user 
        # elif len(non_na_unique) > 1: 
        #     print(f"Inconsistent values found for 'others affected' for family {family_id}: {non_na_unique}")
        
    pedigree = pedigree.drop_duplicates()
            
    # upload
    client.save_schema(table='Pedigree', data=pedigree)

    # work in progress - unfinished
    # updated_records = pedigree.loc[pedigree['is new'] == False].copy() # get the records to be updated
    # current_pedigrees = client.get(table='Pedigree', as_df=True)

    # current_pedigrees_ids = current_pedigrees['id']
    # pedigree['is new pedigree'] = ~pedigree['id'].isin(current_pedigrees_ids)

    # merged = pedigree.merge(current_pedigrees, on='id', how='inner', suffixes = ('_new', '_old'))

    # changed_mask = (merged
    #                 .filter(like='_new')
    #                 .ne(merged.filter(like='_old').values)
    #                 .any(axis=1)
    #                 )
    
    # merged['changed'] = changed_mask

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

    individual_status_dict = {
        'Deceased': 'Dead'
    }
    individuals['individual status'] = individuals['individual status'].replace(
        individual_status_dict)

    # map sex
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
    """ SKIPPED FOR NOW - needs discussion 
    TODO: there are some issues with the API, this function will need to be checked once the issues are fixed
    Map staging area data into the Pedigree members table
    If index = Yes, then relative is itself (i.e., the patient). 
    If index = No, then relative is the individual of the same family with index set to yes.
    If index = No and there is not relative in the same family with index set to yes --> This should not happen and should be reported
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
                print(f'no diseases relative for individual: {member.get('individual')} from family: {famid}')
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

async def match_ontologies(ontology: str, gpap_data: dict, codesystem: bool):
    """Checks for an ontology whether a GPAP value has a RD3 value, based on the code.
    ontology = name of the ontology according to RD3
    gpap_data = dictionary with the GPAP ontology values and corresponding codes
    codesystem = whether the code includes the codesystem in RD3 (eg., HP:0000001)
    """
    molgenis = Client(
        environ['MOLGENIS_HOST'],
        schema='CatalogueOntologies',
        token=environ['MOLGENIS_TOKEN']
    )
    # get phenotypes ontology and overrides
    rd3_ontology = molgenis.get(
        table=ontology, schema='CatalogueOntologies', as_df=True)

    overrides = molgenis.get(
        table='Ontology mappings',
        schema=environ['MOLGENIS_HOST_SCHEMA_SOURCE'],
        as_df=True
    )
    overrides = overrides.loc[overrides['ontology'] == ontology]

    # create dictionary of RD3 code and ontology name
    code_to_name = dict(zip(rd3_ontology['code'], rd3_ontology['name']))

    # save the matches (the once without a perfect name match)
    match_dict = {}
    # and save the ones that do not have a name and/or HP code match
    unmatched = []
    no_correct_name = []
    for ontology_value, code in gpap_data.items():
        if not codesystem:
            code_no_name = code.split(':')[1]
        else:
            code_no_name = code
        # Case 1: name match ignore (we only want mismatched names)
        if ontology_value in rd3_ontology['name'].values:
            continue
        # Case 2: code match save to match_dict
        elif code_no_name in code_to_name:
            match_dict[ontology_value] = code_to_name[code_no_name]
        # Case 3: if the code is not known and has an override
        elif code in overrides['invalid code'].values:
            correct_name = overrides.loc[overrides['invalid code']
                                                  == code]['correct name']
            if pd.notna(correct_name.iloc[0]): # check if there is an overrride
                match_dict[ontology_value] = correct_name.squeeze()
            else:
                no_correct_name.append({
                    'ontology': ontology,
                    'invalid code': code,
                    'invalid name': ontology_value
                })
        # Case 4: no match at all
        else:
            unmatched.append({
                'ontology': ontology,
                'invalid code': code,
                'invalid name': ontology_value
            })

    # upload the new unmatched phenotype ontology values
    output_path = environ['OUTPUT_PATH']
    pd.DataFrame(unmatched).drop_duplicates().to_csv(f'{output_path}Ontology mappings.csv', index=False)
    await molgenis.upload_file(file_path=f'{output_path}Ontology mappings.csv', schema=environ['MOLGENIS_HOST_SCHEMA_SOURCE'])

    # combine the new unmatched phenotypes with the already known unmatched phenotypes (but without a correct name)
    # -- these will be removed from the phenotype observations dataset to be uplaoded
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
                        disease_history2.append(new_entry)
                        # save the disease names with code as prevalent in GPAP
                        diseases_dict[disease.get('ordo').get('name')] = disease.get('ordo').get(
                            'id')

    # convert list to a dataframe
    disease_history = pd.DataFrame(disease_history2)

    diseases_dict, unmatched = asyncio.run(match_ontologies('Diseases', diseases_dict, False))
    disease_history['disease'] = disease_history['disease'].replace(diseases_dict)

    # get the unmatched diseases (the diseases from GPAP not present in RD3)
    # only get the name without the code
    unmatched_names = [entry['invalid name'] for entry in unmatched]
    tmp = disease_history.loc[disease_history['disease'].isin(
        unmatched_names)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 disease equivalent
    disease_history = disease_history.drop(tmp, axis=0)

    # map the disease status
    status_dict = {
        'Confirmed': 'Confirmed diagnosis'
    }
    disease_history['disease status'] = disease_history['disease status'].replace(
        status_dict)

    # map age group at onset
    # TODO: save the records that do not have a ontology term match. --> in the staging area tables save this information
    # any record that does not map should be flagged --> go through it once a month or something
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

    # capture individual's auto id
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
                        pheno_observations2.append(new_entry)
                        # save the phenotype names with code as prevalent in GPAP
                        obs_dict[observation.get('name')] = observation.get(
                            'id')  # only get code (without HP:)

    # convert phenotypic observations list to df
    phen_observations = pd.DataFrame(pheno_observations2)

    # map the phenotypic features from GPAP format to RD3
    phen_dict, unmatched = asyncio.run(match_ontologies('Phenotypes', obs_dict, True))
    phen_observations['type'] = phen_observations['type'].replace(phen_dict)

    # get the unmatched phenotypes (the phenotypes from GPAP not present in RD3)
    # only get the name without the HP code
    unmatched_names = [entry['invalid name'] for entry in unmatched]
    tmp = phen_observations.loc[phen_observations['type'].isin(
        unmatched_names)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 phenotype equivalent
    phen_observations = phen_observations.drop(tmp, axis=0)

    # check if the data is correct
    # create a count for the phenotypes per individual - should be 1 for everyone 
    result = phen_observations.drop_duplicates().groupby(['part of clinical observation', 'type']).size().reset_index(name='count')
    logging.info('Checking whether the data is correct')
    to_report = result[result['count'] > 1]
    for index, row in to_report.iterrows():
        partOfClinObs = row['part of clinical observation']
        type = row['type']
        print(f'For individual {partOfClinObs} the phenotype {type} is both observed and unobserved.')
        indices_to_remove = phen_observations.loc[(phen_observations['part of clinical observation'] == partOfClinObs) & (phen_observations['type'] == type)].index
        phen_observations = phen_observations.drop(indices_to_remove)

    # upload
    client.save_schema(table='Phenotype observations',
                       data=phen_observations.drop_duplicates())

    
def build_import_variants(client: Client, data: pd.DataFrame):
    """Builds and import variant information from GPAP to RD3
    # disabled for now -- unfinished"""

    variant_interpretations = data[[
        'inheritance'
    ]]

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
    genomic_variant = data[['regions']].copy() # index 67

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
