"""RD3 Staging area mapping scripts
"""
from datetime import datetime
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


def get_new_participants(data: pd.DataFrame):
    """Get the new participants based on the last_modification_date
    TEST
    """
    previous_date = '2024-12-18T14:42:04.125522'  # to test
    previous_date = datetime.fromisoformat(previous_date)

    # convert the last_modification_date into datetime format
    data['last_modification_date'] = data['last_modification_date'].astype(
        'str')
    data['last_modification_date'] = data['last_modification_date'].apply(lambda x:
                                                                          datetime.fromisoformat(
                                                                              x)
                                                                          )

    # get only the new data
    data = data[data['last_modification_date'] > previous_date]
    return data


def build_import_pedigree_table(client, data: pd.DataFrame):
    """Map staging area data into the Pedigree table format"""
    pedigree = data[['famid']] \
        .rename(columns={'famid': 'id'}) \
        .drop_duplicates() \
        .sort_values(by="id")

    # get the pedigree information with family_id (a.k.a alternate ids) and the others affacted info
    pedigree_alternates = data[['famid', 'family_id', 'otheraffected']]

    others_affected_dict = {
        'Yes': True,
        'No': False
    }

    pedigree['others affected'] = pedigree_alternates['otheraffected'].map(
        others_affected_dict)

    # initialise the alternate ids column
    pedigree['alternate ids'] = None
    for index, family in pedigree.iterrows():
        # retrieve the family IDs for this family
        alternate_ids = pedigree_alternates.loc[pedigree_alternates['famid']
                                                == family['id'], 'family_id'].to_list()
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

    # convert list to a dataframe
    disease_history = pd.DataFrame(disease_history2)

    # map the diseases to the EMX2 term
    disease_dict = {
        'Ullrich congenital muscular dystrophy': 'Congenital muscular dystrophy, Ullrich type',
        'Calpain-3-related limb-girdle muscular dystrophy R1': 'Calpain-3-related  limb-girdle muscular dystrophy R1'
    }
    unmatched = ['valosin containing protein']
    disease_history['disease'] = disease_history['disease'].replace(
        disease_dict)

    # get the indices of the rows to remove (no match)
    tmp = disease_history.loc[disease_history['disease'].isin(unmatched)].index
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
    client.save_schema(table='Disease history', data=disease_history)
    client.save_schema(table='Clinical observations', data=clinical_obs)


async def match_phenotypes(obs: dict):
    """Match phenotypes from GPAP with phenotypes in RD3
    If the name is not a perfect match, see if the HP code is present in the RD3 ontology, 
    if not, save the ones that do not have a match. """
    molgenis = Client(
        environ['MOLGENIS_HOST'],
        schema='CatalogueOntologies',
        token=environ['MOLGENIS_TOKEN']
    )
    # get phenotypes ontology and overrides
    phenotypes = molgenis.get(
        table='Phenotypes', schema='CatalogueOntologies', as_df=True)

    hpo_overrides = molgenis.get(
        table='Ontology mappings',
        schema='new staging area',
        as_df=True
    )
    hpo_overrides = hpo_overrides.loc[hpo_overrides['ontology'] == 'HPO']

    # create dictionary of RD3 code and ontology name
    code_to_name = dict(zip(phenotypes['code'], phenotypes['name']))

    # save the matches (the once without a perfect name match)
    match_dict = {}
    # and save the ones that do not have a name and/or HP code match
    unmatched = []
    no_correct_name = []
    for pheno, code in obs.items():
        HP_code = 'HP:'+code
        # Case 1: name match ignore (we only want mismatched names)
        if pheno in phenotypes['name'].values:
            continue
        # Case 2: code match save to match_dict
        elif code in code_to_name:
            match_dict[pheno] = code_to_name[code]
        # Case 3: if the code is not known and has an override
        elif HP_code in hpo_overrides['invalid code'].values:
            correct_name = hpo_overrides.loc[hpo_overrides['invalid code']
                                                  == HP_code]['correct name']
            if pd.notna(correct_name.iloc[0]): # check if there is an overrride
                match_dict[pheno] = correct_name.squeeze()
            else:
                no_correct_name.append({
                    'ontology': 'HPO',
                    'invalid code': HP_code,
                    'invalid name': pheno
                })
        # Case 4: no match at all
        else:
            unmatched.append({
                'ontology': 'HPO',
                'invalid code': HP_code,
                'invalid name': pheno
            })

    # upload the new unmatched phenotype ontology values
    output_path = environ['OUTPUT_PATH']
    pd.DataFrame(unmatched).drop_duplicates().to_csv(f'{output_path}Ontology mappings.csv', index=False)
    await molgenis.upload_file(file_path=f'{output_path}Ontology mappings.csv', schema="new staging area")

    # combine the new unmatched phenotypes with the already known unmatched phenotypes (but without a correct name)
    # -- these will be removed from the phenotype observations dataset to be uplaoded
    unmatched = unmatched + no_correct_name

    return match_dict, unmatched


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
                            'id').split(':')[1]  # only get code (without HP:)

    # convert phenotypic observations list to df
    phen_observations = pd.DataFrame(pheno_observations2)

    # map the phenotypic features from GPAP format to RD3
    phen_dict, unmatched = asyncio.run(match_phenotypes(obs_dict))
    phen_observations['type'] = phen_observations['type'].replace(phen_dict)

    # get the unmatched phenotypes (the phenotypes from GPAP not present in RD3)
    # only get the name without the HP code
    unmatched_names = [entry['invalid name'] for entry in unmatched]
    tmp = phen_observations.loc[phen_observations['type'].isin(
        unmatched_names)].index  # get the indices of the rows to remove (no match)
    # remove rows without a RD3 phenotype equivalent
    phen_observations = phen_observations.drop(tmp, axis=0)

    # upload
    client.save_schema(table='Phenotype observations',
                       data=phen_observations.drop_duplicates())

    # import unknown HPO mappings
    # unmatched_df = pd.DataFrame(unmatched).drop_duplicates()
    # client.save_schema(table='Ontology mappings', data=unmatched_df)


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
