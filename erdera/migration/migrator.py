"""
Script to publish data from staging areas to a the ERDERA production database.
"""
import asyncio
from dotenv import load_dotenv
import logging
import os
from pathlib import Path
import shutil
import zipfile

import pandas as pd

from molgenis_emx2_pyclient import Client

load_dotenv()

SERVER_URL = 'http://localhost:8080/'
TOKEN = os.environ.get("MOLGENIS_TOKEN")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE")

if os.environ.get('MOLGENIS_HOST'):
    SERVER_URL = os.environ['MOLGENIS_HOST']

TARGET = 'erdera' # production db

# Set up the logger
logging.basicConfig(level='INFO', filename=OUTPUT_FILE)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
log = logging.getLogger('publisher')

async def publish(staging_area: str, client: Client):
    """
    Simple program to publish data from a staging area to the 'erdera' production db.

    Downloads the content of the staging area in ZIP format.
    Unpacks the ZIP file, iterates over its tables.
    Removes molgenis-related and empty tables.
    Leave only the non-draft records.
    Combines the results in a ZIP.
    Uploads the ZIP to the erdera db.
    """
    # Export staging area data
    await client.export(schema=staging_area, filename=str(Path(__file__).parent / "source.zip"))
    zf = zipfile.ZipFile(Path(__file__).parent / "source.zip", 'r') # read the zipped archive
    # create new folder (if not exists) to extract the zipped data to
    data_directory = Path(__file__).parent / "data"
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    # extract all data into a folder
    zf.extractall(path=data_directory)

    for file in Path(data_directory).iterdir():
        # do not migrate
        if file.name in ["molgenis.csv", "molgenis_members.csv", "molgenis_settings.csv"]:
            os.remove(file)
            continue
        # read the data
        data = pd.read_csv(file)
        if len(data) == 0: # empty (no data  to migrate)
            os.remove(file)
            continue
        
        # check if there is any draft data
        if data['mg_draft'].any():
            log.warning(f'There are draft entries in {file.stem} of staging area {staging_area}. No data is migrated.')
            return # if this is the case, the data won't be migrated
        
    # zip the folder and upload to the target database
    shutil.make_archive(f'{data_directory.parent}/data', 'zip', data_directory)

    # Upload zip to TARGET schema
    await client.upload_file(data_directory.parent / "data.zip", schema=TARGET)

    # delete the migrated records from the staging area by setting the mg_delete to true 
    for file in Path(data_directory).iterdir():
        data = pd.read_csv(file)
        data['mg_delete'] = True
        data.to_csv(data_directory / file, index=False)
    
    # zip the folder with the data to delete from the staging area
    shutil.make_archive(f'{data_directory.parent}/data', 'zip', data_directory)

    # upload zip to staging area to delete the records
    log.info(f'Deleting data from {[f.stem for f in data_directory.iterdir()]} of {staging_area}')
    await client.upload_file(f"{data_directory.parent}/data.zip", schema=staging_area)

    # delete the data folder and its contents.
    shutil.rmtree(data_directory)
    # delete the archives
    os.remove(data_directory.parent / "data.zip")
    os.remove(data_directory.parent / "source.zip")
 
def get_staging_areas(client: Client):
    """
    Get the staging areas to migrate 
    """
    # get all schemas
    schemas = client.get_schemas()
    schema_ids = [schema.id for schema in schemas]

    # should not be migrated
    exclude = {
        "Staging area GPAP",
        "Staging area EGA",
        "Staging area ids",
    }

    # get the schemas to be migrated
    schemas = [
        schema for schema in schema_ids
        if schema.startswith("Staging area") and schema not in exclude
    ]
    return schemas

def publish_all():
    """
    Publish all data from the staging areas to the production RD3 database
    """
    #client = Client(url=SERVER_URL, token=TOKEN, job="${jobId}") # use when running the script on a server
    client = Client(url=SERVER_URL, token=TOKEN) # use when running the script locally

    staging_areas = get_staging_areas(client)
    
    # loop through the staging areas
    for staging_area in staging_areas:
        print(f"Publishing {staging_area!r}.")
        try:
            asyncio.run(publish(staging_area=staging_area, client=client))
        except Exception as e:
            print("An error occurred.")
            log.error(e)
    client.session.close()

if __name__ == '__main__':
    publish_all()