"""Functional tests for erdera/mapping/GPAP/mapping_cnag_experiments_to_rd3.py

The script is run as a whole against a fake MOLGENIS server. The tests describe
the current behaviour, so that later changes to the script are deliberate.
"""
import logging

import pandas as pd
import pytest

SCRIPT = 'erdera/mapping/GPAP/mapping_cnag_experiments_to_rd3.py'

STAGING = 'Staging Area Gpap'
MAPPINGS = 'Ontology mappings'
ONTOLOGIES = 'CatalogueOntologies'
TARGET = 'erdera'
SOURCE_PREFIX = 'datamanagement_service/api/experimentsview/'


def experiment(experiment_id, participant, **values):
    record = {
        'ExperimentID': experiment_id,
        'LocalExperimentID': f'local-{experiment_id}',
        'kit': 'Kit A',
        'project': 'ERDERA',
        'subproject': 'ERDERA_PF1',
        'Participant_ID': participant,
        'library_strategy': 'WES',
        'Sample_ID': f'sample-{experiment_id}',
        'library_source': 'GENOMIC',
        'tissue': 'Blood',
        'erns': 'ERN-RND',
        'Owner': 'Lab A',
    }
    record.update(values)
    return record


DEFAULT_EXPERIMENTS = [
    experiment('E1', 'P1'),
    experiment('E2', 'P1', subproject='ERDERA_PF2', library_strategy='WGS',
               tissue='Skin', erns='ERN-ITHACA', Owner='Lab B', kit='Kit B'),
    experiment('E3', 'P2', project='Solve-RD', subproject='TOPFANA_03'),
]


def mappings(pairs: dict) -> pd.DataFrame:
    return pd.DataFrame({'incoming value': list(pairs), 'new value': list(pairs.values())})


def seed(molgenis, experiments=None):
    """Fill the fake server with a small, consistent data set.

    The staging columns are of type 'string' in model/staging_area_gpap, which the
    pyclient returns with the pandas 'string' dtype.
    """
    molgenis.add_table(STAGING, 'Experiments',
                       pd.DataFrame(experiments or DEFAULT_EXPERIMENTS).astype('string'))

    molgenis.add_table(MAPPINGS, 'Tissue types', mappings({'Blood': 'Whole blood'}))
    molgenis.add_table(MAPPINGS, 'Erns', mappings({'ERN-RND': 'ERN RND', 'ERN-ITHACA': 'ERN ITHACA'}))
    molgenis.add_table(MAPPINGS, 'Experiment types', mappings({'WES': 'Whole exome sequencing'}))
    molgenis.add_table(MAPPINGS, 'Library source', mappings({'GENOMIC': 'Genomic'}))
    molgenis.add_table(MAPPINGS, 'Organisations', mappings({
        'Lab A': 'University A',
        'Lab B': 'University B',
        'Lab C': 'University C',
    }))

    molgenis.add_table(ONTOLOGIES, 'Organisations', pd.DataFrame({
        'name': ['Lab A', 'University A', 'Lab C'],
        'parent': ['University A', None, 'Old university C'],
    }))

    molgenis.add_table(TARGET, 'Collections', pd.DataFrame({
        'id': ['ERDERA', 'Solve-RD', 'ERDERA_PF1'],
        'name': ['ERDERA', 'Solve-RD', 'ERDERA_PF1'],
        'type': ['Registry', 'Registry', 'Registry'],
    }))

    molgenis.add_table(TARGET, 'Individuals', pd.DataFrame({
        'id': ['P1', 'P2', 'P3'],
        'gender at birth': ['female', 'male', 'female'],
        'submitter': [None, None, 'Lab Z'],
        'affiliated networks': [None, None, 'ERN Z'],
        'included in resources': [None, None, 'Collection Z'],
    }))


def rows(data: pd.DataFrame, key: str = 'id') -> list:
    """The uploaded records sorted by key, with missing values as None."""
    data = data.sort_values(key).astype(object)
    return data.where(data.notna(), None).to_dict('records')


@pytest.fixture
def run_mapping(molgenis, run_script):
    def seed_and_run(experiments=None):
        seed(molgenis, experiments)
        run_script(SCRIPT)
        return molgenis
    return seed_and_run


def test_connects_with_host_and_token_from_environment(run_mapping):
    molgenis = run_mapping()

    assert molgenis.clients
    assert {client.url for client in molgenis.clients} == {'https://molgenis.test/'}
    assert {client.token for client in molgenis.clients} == {'test-token'}


def test_uploads_to_target_schema_in_dependency_order(run_mapping):
    molgenis = run_mapping()

    target_tables = [upload.table for upload in molgenis.uploads if upload.schema == TARGET]
    assert target_tables == [
        'Collections', 'Samples srDNA', 'Collections', 'Individuals', 'Experiments srDNA']


def test_uploads_samples_with_mapped_ontology_terms(run_mapping):
    samples = run_mapping().upload_to(TARGET, 'Samples srDNA')

    assert list(samples.columns) == [
        'tissue type', 'Sample_ID', 'individuals', 'id', 'affiliated networks', 'submitter',
        'included in resources']
    assert rows(samples) == [
        {'tissue type': 'Whole blood', 'Sample_ID': 'sample-E1', 'individuals': 'P1', 'id': 'E1',
         'affiliated networks': 'ERN RND', 'submitter': 'Lab A',
         'included in resources': 'ERDERA,ERDERA_PF1'},
        {'tissue type': None, 'Sample_ID': 'sample-E2', 'individuals': 'P1', 'id': 'E2',
         'affiliated networks': 'ERN ITHACA', 'submitter': 'Lab B',
         'included in resources': 'ERDERA,ERDERA_PF2'},
        {'tissue type': 'Whole blood', 'Sample_ID': 'sample-E3', 'individuals': 'P2', 'id': 'E3',
         'affiliated networks': 'ERN RND', 'submitter': 'Lab A',
         'included in resources': 'Solve-RD'},
    ]


def test_uploads_experiments(run_mapping):
    experiments = run_mapping().upload_to(TARGET, 'Experiments srDNA')

    assert list(experiments.columns) == [
        'id', 'alternate ids', 'target enrichment kit', 'individuals', 'library strategy',
        'sample', 'library source', 'submitter', 'affiliated networks',
        'included in resources', 'local experiment id']
    assert rows(experiments) == [
        {'id': 'E1', 'alternate ids': 'local-E1', 'target enrichment kit': 'Kit A',
         'individuals': 'P1', 'library strategy': 'Whole exome sequencing', 'sample': 'E1',
         'library source': 'Genomic', 'submitter': 'Lab A', 'affiliated networks': 'ERN RND',
         'included in resources': 'ERDERA,ERDERA_PF1', 'local experiment id': 'E1'},
        {'id': 'E2', 'alternate ids': 'local-E2', 'target enrichment kit': 'Kit B',
         'individuals': 'P1', 'library strategy': None, 'sample': 'E2',
         'library source': 'Genomic', 'submitter': 'Lab B', 'affiliated networks': 'ERN ITHACA',
         'included in resources': 'ERDERA,ERDERA_PF2', 'local experiment id': 'E2'},
        {'id': 'E3', 'alternate ids': 'local-E3', 'target enrichment kit': 'Kit A',
         'individuals': 'P2', 'library strategy': 'Whole exome sequencing', 'sample': 'E3',
         'library source': 'Genomic', 'submitter': 'Lab A', 'affiliated networks': 'ERN RND',
         'included in resources': 'Solve-RD', 'local experiment id': 'E3'},
    ]


@pytest.mark.parametrize('project, subproject, expected', [
    ('ERDERA', 'ERDERA_PF1', 'ERDERA,ERDERA_PF1'),
    ('ERDERA', 'ERDERA_PF1_batch2', 'ERDERA,ERDERA_PF1'),
    ('ERDERA', 'ERDERA_PF2', 'ERDERA,ERDERA_PF2'),
    ('ERDERA', 'ERDERA_PF3', 'ERDERA,ERDERA_PF3'),
    ('Solve-RD', 'TOPFANA_01', 'Solve-RD'),
    ('Solve-RD', 'TOPFANA_02', 'Solve-RD'),
    ('Solve-RD', 'TOPFANA_03', 'Solve-RD'),
    ('Solve-RD', 'TOPFANA_04', 'Solve-RD'),
    ('Solve-RD ERDERA', 'ERDERA_PF2', 'Solve-RD,ERDERA,ERDERA_PF2'),
    ('ERDERA', 'OTHER_SUBPROJECT', 'ERDERA'),
    ('ERDERA', None, 'ERDERA'),
    ('OTHER_PROJECT', None, ''),
])
def test_maps_project_and_subproject_to_included_in_resources(
        run_mapping, project, subproject, expected):
    molgenis = run_mapping([experiment('E1', 'P1', project=project, subproject=subproject)])

    for table in ('Samples srDNA', 'Experiments srDNA'):
        uploaded = molgenis.upload_to(TARGET, table)
        assert uploaded['included in resources'].tolist() == [expected], table


def test_logs_subprojects_that_are_not_a_freeze(run_mapping, caplog):
    caplog.set_level(logging.INFO)

    run_mapping([
        experiment('E1', 'P1', subproject='ERDERA_PF1'),
        experiment('E2', 'P1', subproject='TOPFANA_01'),
        experiment('E3', 'P2', subproject='TOPFANA_01'),
        experiment('E4', 'P2', subproject=None),
    ])

    assert ("Found subproject not matching the ERDERA freeze format: ['TOPFANA_01']"
            in caplog.messages)


def test_adds_freezes_from_the_data_that_are_not_collections_yet(run_mapping):
    molgenis = run_mapping([
        experiment('E1', 'P1', subproject='ERDERA_PF1'),
        experiment('E2', 'P1', subproject='ERDERA_PF3'),
        experiment('E3', 'P2', subproject='ERDERA_PF3_batch2'),
    ])

    new_freezes = molgenis.uploads_to(TARGET, 'Collections')[0]
    assert rows(new_freezes) == [
        {'name': 'ERDERA_PF3', 'id': 'ERDERA_PF3', 'type': 'Registry',
         'description': 'Data freeze 3'},
    ]


def test_adds_no_freezes_when_all_freezes_are_collections(run_mapping):
    molgenis = run_mapping([experiment('E1', 'P1', subproject='ERDERA_PF1')])

    assert len(molgenis.uploads_to(TARGET, 'Collections')) == 1


def test_adds_projects_and_first_freezes_as_collections(run_mapping):
    collections = run_mapping().uploads_to(TARGET, 'Collections')[-1]

    assert rows(collections) == [
        {'id': 'ERDERA', 'name': 'ERDERA',
         'description': 'European Rare Diseases Research Alliance', 'type': 'Registry'},
        {'id': 'ERDERA_PF1', 'name': 'ERDERA_PF1', 'description': 'Data freeze 1', 'type': 'Registry'},
        {'id': 'ERDERA_PF2', 'name': 'ERDERA_PF2', 'description': 'Data freeze 2', 'type': 'Registry'},
        {'id': 'Solve-RD', 'name': 'Solve-RD',
         'description': 'Solving the Unsolved Rare Diseases', 'type': 'Registry'},
    ]


def test_sets_submitter_networks_and_resources_on_all_individuals(run_mapping):
    individuals = run_mapping().upload_to(TARGET, 'Individuals')

    assert rows(individuals) == [
        {'id': 'P1', 'gender at birth': 'female', 'submitter': 'Lab A,Lab B',
         'affiliated networks': 'ERN RND,ERN ITHACA',
         'included in resources': 'ERDERA,ERDERA_PF1,ERDERA_PF2'},
        {'id': 'P2', 'gender at birth': 'male', 'submitter': 'Lab A',
         'affiliated networks': 'ERN RND', 'included in resources': 'Solve-RD'},
        {'id': 'P3', 'gender at birth': 'female', 'submitter': None,
         'affiliated networks': None, 'included in resources': None},
    ]


def test_combines_values_of_individuals_without_duplicates_or_empty_values(run_mapping):
    individuals = run_mapping([
        experiment('E1', 'P1', subproject='ERDERA_PF1'),
        experiment('E2', 'P1', subproject='ERDERA_PF1'),
        experiment('E3', 'P1', project='OTHER_PROJECT', subproject=None, erns=None),
    ]).upload_to(TARGET, 'Individuals')

    assert rows(individuals)[0] == {
        'id': 'P1', 'gender at birth': 'female', 'submitter': 'Lab A',
        'affiliated networks': 'ERN RND', 'included in resources': 'ERDERA,ERDERA_PF1'}


def test_reports_unmatched_ontology_values(run_mapping):
    molgenis = run_mapping()

    assert rows(molgenis.upload_to(MAPPINGS, 'Tissue types'), 'incoming value') == [
        {'incoming value': 'Skin', 'source': f'{SOURCE_PREFIX}tissue'}]
    assert rows(molgenis.upload_to(MAPPINGS, 'Experiment types'), 'incoming value') == [
        {'incoming value': 'WGS', 'source': f'{SOURCE_PREFIX}library_strategy'}]


def test_reports_nothing_when_all_ontology_values_match(run_mapping):
    molgenis = run_mapping()

    for table in ('Erns', 'Library source'):
        uploads = molgenis.uploads_to(MAPPINGS, table)
        assert all(data.empty for data in uploads), table


def test_adds_new_submitters_and_updates_parents_of_organisations(run_mapping):
    organisations = run_mapping().upload_to(ONTOLOGIES, 'Organisations')

    assert rows(organisations, 'name') == [
        {'name': 'Lab B', 'parent': 'University B'},
        {'name': 'Lab C', 'parent': 'University C'},
    ]


def test_adds_new_submitters_to_organisation_mappings(run_mapping):
    organisation_mappings = run_mapping().upload_to(MAPPINGS, 'Organisations')

    assert rows(organisation_mappings, 'incoming value') == [
        {'incoming value': 'Lab B', 'new value': 'University B', 'source': 'Owner'}]
