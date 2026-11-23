"""Functional tests for erdera/mapping/GPAP/mapping_cnag_experiments_to_rd3.py

The script is run as a whole against a fake MOLGENIS server. The tests describe
the current behaviour, so that later changes to the script are deliberate.
"""
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
    }))


def rows(data: pd.DataFrame, key: str = 'id') -> list:
    """The uploaded records sorted by key, with missing values as None."""
    data = data.sort_values(key).astype(object)
    return data.where(data.notna(), None).to_dict('records')


@pytest.fixture
def run(molgenis, run_script):
    def run_with(experiments=None):
        seed(molgenis, experiments)
        run_script(SCRIPT)
        return molgenis
    return run_with


def test_connects_with_host_and_token_from_environment(run):
    molgenis = run()

    assert molgenis.clients
    assert {client.url for client in molgenis.clients} == {'https://molgenis.test/'}
    assert {client.token for client in molgenis.clients} == {'test-token'}


def test_uploads_to_target_schema_in_dependency_order(run):
    molgenis = run()

    target_tables = [upload.table for upload in molgenis.uploads if upload.schema == TARGET]
    assert target_tables == ['Samples srDNA', 'Collections', 'Individuals', 'Experiments srDNA']


def test_uploads_samples_with_mapped_ontology_terms(run):
    samples = run().upload_to(TARGET, 'Samples srDNA')

    assert list(samples.columns) == [
        'tissue type', 'Sample_ID', 'individuals', 'id', 'affiliated networks', 'submitter']
    assert rows(samples) == [
        {'tissue type': 'Whole blood', 'Sample_ID': 'sample-E1', 'individuals': 'P1', 'id': 'E1',
         'affiliated networks': 'ERN RND', 'submitter': 'Lab A'},
        {'tissue type': None, 'Sample_ID': 'sample-E2', 'individuals': 'P1', 'id': 'E2',
         'affiliated networks': 'ERN ITHACA', 'submitter': 'Lab B'},
        {'tissue type': 'Whole blood', 'Sample_ID': 'sample-E3', 'individuals': 'P2', 'id': 'E3',
         'affiliated networks': 'ERN RND', 'submitter': 'Lab A'},
    ]


def test_uploads_experiments(run):
    experiments = run().upload_to(TARGET, 'Experiments srDNA')

    assert list(experiments.columns) == [
        'id', 'alternate ids', 'target enrichment kit', 'individuals', 'library strategy',
        'sample', 'library source', 'included in resources', 'submitter',
        'affiliated networks', 'local experiment id']
    assert rows(experiments) == [
        {'id': 'E1', 'alternate ids': 'local-E1', 'target enrichment kit': 'Kit A',
         'individuals': 'P1', 'library strategy': 'Whole exome sequencing', 'sample': 'E1',
         'library source': 'Genomic', 'included in resources': 'ERDERA,ERDERA_PF1',
         'submitter': 'Lab A,Lab B', 'affiliated networks': 'ERN RND,ERN ITHACA',
         'local experiment id': 'E1'},
        {'id': 'E2', 'alternate ids': 'local-E2', 'target enrichment kit': 'Kit B',
         'individuals': 'P1', 'library strategy': None, 'sample': 'E2',
         'library source': 'Genomic', 'included in resources': 'ERDERA,ERDERA_PF2',
         'submitter': 'Lab A,Lab B', 'affiliated networks': 'ERN RND,ERN ITHACA',
         'local experiment id': 'E2'},
        {'id': 'E3', 'alternate ids': 'local-E3', 'target enrichment kit': 'Kit A',
         'individuals': 'P2', 'library strategy': 'Whole exome sequencing', 'sample': 'E3',
         'library source': 'Genomic', 'included in resources': 'Solve-RD,ERDERA_PF2',
         'submitter': 'Lab A', 'affiliated networks': 'ERN RND',
         'local experiment id': 'E3'},
    ]


@pytest.mark.parametrize('project, subproject, expected', [
    ('ERDERA', 'ERDERA_PF1', 'ERDERA,ERDERA_PF1'),
    ('ERDERA', 'ERDERA_PF1_batch2', 'ERDERA,ERDERA_PF1'),
    ('ERDERA', 'ERDERA_PF2', 'ERDERA,ERDERA_PF2'),
    ('Solve-RD', 'TOPFANA_01', 'Solve-RD,ERDERA_PF2'),
    ('Solve-RD', 'TOPFANA_02', 'Solve-RD,ERDERA_PF2'),
    ('Solve-RD', 'TOPFANA_03', 'Solve-RD,ERDERA_PF2'),
    ('Solve-RD', 'TOPFANA_04', 'Solve-RD,ERDERA_PF2'),
    ('Solve-RD ERDERA', 'ERDERA_PF2', 'Solve-RD,ERDERA,ERDERA_PF2'),
    ('ERDERA', 'OTHER_SUBPROJECT', 'ERDERA,OTHER_SUBPROJECT'),
    ('ERDERA', None, 'ERDERA'),
    ('OTHER_PROJECT', None, ''),
])
def test_maps_project_and_subproject_to_included_in_resources(run, project, subproject, expected):
    molgenis = run([experiment('E1', 'P1', project=project, subproject=subproject)])

    experiments = molgenis.upload_to(TARGET, 'Experiments srDNA')
    assert experiments['included in resources'].tolist() == [expected]


def test_adds_projects_and_freezes_as_collections(run):
    collections = run().upload_to(TARGET, 'Collections')

    assert rows(collections) == [
        {'id': 'ERDERA', 'name': 'ERDERA',
         'description': 'European Rare Diseases Research Alliance', 'type': 'Registry'},
        {'id': 'ERDERA_PF1', 'name': 'ERDERA_PF1', 'description': 'Data freeze 1', 'type': 'Registry'},
        {'id': 'ERDERA_PF2', 'name': 'ERDERA_PF2', 'description': 'Data freeze 2', 'type': 'Registry'},
        {'id': 'Solve-RD', 'name': 'Solve-RD',
         'description': 'Solving the Unsolved Rare Diseases', 'type': 'Registry'},
    ]


def test_sets_submitter_and_networks_on_all_individuals(run):
    individuals = run().upload_to(TARGET, 'Individuals')

    assert rows(individuals) == [
        {'id': 'P1', 'gender at birth': 'female',
         'submitter': 'Lab A,Lab B', 'affiliated networks': 'ERN RND,ERN ITHACA'},
        {'id': 'P2', 'gender at birth': 'male',
         'submitter': 'Lab A', 'affiliated networks': 'ERN RND'},
        {'id': 'P3', 'gender at birth': 'female',
         'submitter': None, 'affiliated networks': None},
    ]


def test_reports_unmatched_ontology_values(run):
    molgenis = run()

    assert rows(molgenis.upload_to(MAPPINGS, 'Tissue types'), 'incoming value') == [
        {'incoming value': 'Skin', 'source': f'{SOURCE_PREFIX}tissue'}]
    assert rows(molgenis.upload_to(MAPPINGS, 'Experiment types'), 'incoming value') == [
        {'incoming value': 'WGS', 'source': f'{SOURCE_PREFIX}library_strategy'}]


def test_reports_nothing_when_all_ontology_values_match(run):
    molgenis = run()

    for table in ('Erns', 'Library source'):
        uploads = molgenis.uploads_to(MAPPINGS, table)
        assert all(data.empty for data in uploads), table


def test_adds_new_submitters_and_updates_parents_of_organisations(run):
    organisations = run().upload_to(ONTOLOGIES, 'Organisations')

    assert rows(organisations, 'name') == [
        {'name': 'Lab B', 'parent': 'University B'},
        {'name': 'Lab C', 'parent': 'University C'},
    ]


def test_adds_new_submitters_to_organisation_mappings(run):
    organisation_mappings = run().upload_to(MAPPINGS, 'Organisations')

    assert rows(organisation_mappings, 'incoming value') == [
        {'incoming value': 'Lab B', 'new value': 'University B', 'source': 'Owner'}]
