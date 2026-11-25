"""Shared fixtures for the Python tests of the ERDERA scripts.

The scripts are run as they are, without a MOLGENIS server: the pyclient `Client`
is replaced by a fake that serves tables from memory and records all uploads.
"""
import runpy
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import dotenv
import molgenis_emx2_pyclient.client
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Upload:
    """A table upload that a script sent to the fake server."""
    schema: str
    table: str
    data: pd.DataFrame


@dataclass
class FakeMolgenis:
    """In-memory stand-in for a MOLGENIS EMX2 server."""
    tables: dict = field(default_factory=dict)
    uploads: list = field(default_factory=list)
    clients: list = field(default_factory=list)

    def add_table(self, schema: str, table: str, data: pd.DataFrame):
        self.tables[(schema, table)] = data

    def uploads_to(self, schema: str, table: str) -> list:
        return [upload.data for upload in self.uploads
                if upload.schema == schema and upload.table == table]

    def upload_to(self, schema: str, table: str) -> pd.DataFrame:
        """The single non-empty upload to a table."""
        uploads = [data for data in self.uploads_to(schema, table) if not data.empty]
        assert len(uploads) == 1, f'expected one upload to {schema}::{table}, got {len(uploads)}'
        return uploads[0]

    def client_class(self):
        server = self

        class FakeClient:
            def __init__(self, url: str, schema: str = None, token: str = None, job: str = None):
                self.url = url
                self.schema = schema
                self.token = token
                server.clients.append(self)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return None

            def _schema(self, schema):
                current = schema or self.schema
                assert current is not None, 'no schema given and no default schema set'
                return current

            def get(self, table: str, schema: str = None, columns: list = None,
                    query_filter: str = None, as_df: bool = False):
                key = (self._schema(schema), table)
                assert key in server.tables, f'unknown table {key}'
                data = server.tables[key].copy()
                if columns is not None:
                    data = data[columns]
                return data if as_df else data.to_dict('records')

            def save_table(self, table: str, schema: str = None, file=None, data=None):
                server.uploads.append(
                    Upload(self._schema(schema), table, pd.DataFrame(data).copy()))

            async def upload_file(self, file_path, schema: str = None):
                with zipfile.ZipFile(file_path) as archive:
                    for name in archive.namelist():
                        with archive.open(name) as csv_file:
                            data = pd.read_csv(csv_file, keep_default_na=False, na_values=[''])
                        server.uploads.append(
                            Upload(self._schema(schema), Path(name).stem, data))

        return FakeClient


@pytest.fixture
def molgenis(monkeypatch, tmp_path) -> FakeMolgenis:
    """A fake MOLGENIS server, with the environment that the scripts need."""
    server = FakeMolgenis()
    monkeypatch.setattr(molgenis_emx2_pyclient.client, 'Client', server.client_class())
    monkeypatch.setattr(dotenv, 'load_dotenv', lambda *args, **kwargs: None)
    monkeypatch.setenv('MOLGENIS_HOST', 'https://molgenis.test/')
    monkeypatch.setenv('MOLGENIS_TOKEN', 'test-token')
    monkeypatch.setenv('OUTPUT_FILE', str(tmp_path / 'output.log'))
    return server


@pytest.fixture
def run_script():
    """Run a script from the repository as `__main__`, as the script editor does."""
    def run_as_main(relative_path: str):
        runpy.run_path(str(REPO_ROOT / relative_path), run_name='__main__')
    return run_as_main
