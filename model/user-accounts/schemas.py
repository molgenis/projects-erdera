"""
## About

Retrieve a list of the available schemas on an emx2 server, and import the
data into the Users.Schemas table.

## Implementation steps

- Import the molgenis.csv for the Users model
- Copy the contents of this file into a new molgenis script and run
- Import the rest of the data (Users, Permissions, etc.)

## Notes

You can also run this script locally manually setting the `MOLGENIS_TOKEN`

## Requirements (copy into the script editor)

requests
pandas

"""
import os
import sys
from typing import TypedDict, Optional
import logging
import asyncio
import csv
import requests
import pandas as pd

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger("Set schemas:")

MOLGENIS_TOKEN = os.getenv('MOLGENIS_TOKEN')


class ISchema(TypedDict):
    """Typing for Molgenis schema information"""
    id: Optional[str]
    name: str
    label: Optional[str]
    description: str


def transform_schemas(data: list[ISchema] = None, exclude_schemas: list[str] = ['pet store', '_SYSTEM_']):
    """Transform schemas into Users.Schemas format"""
    if data:
        data_copy = data.copy()
        filtered_schemas = [
            {'name': schema['name'], 'description': schema['description']}
            for schema in data_copy
            if schema['name'] not in exclude_schemas
        ]
        return filtered_schemas
    return None


async def get_schemas(client: requests.Session, token: str):
    """Get schemas from central graphql API"""
    log.info('Retrieving available schemas...')
    query = '{ _schemas { id name label description }}'
    response = client.post(
        'http://localhost:8080/api/graphql',
        headers={
            'Content-Type': 'application/json',
            'x-molgenis-token': token
        },
        json={'query': query}
    )
    data = response.json()

    if data.get('data', {}).get('_schemas'):
        schemas = data['data']['_schemas']
        return transform_schemas(schemas)

    return None


async def post_schemas(
    client: requests.Session,
    schema: str = 'Users',
    data: list[ISchema] = None,
    token: str = None
):
    """Import data into Schema/Users"""
    log.info('Importing %s schemas into the %s.Users', len(data), schema)
    import_data = data.copy()

    url = f'http://localhost:8080/{schema}/api/csv/Schemas'
    response = client.post(
        url,
        headers={
            'Content-Type': 'text/csv',
            'x-molgenis-token': token
        },
        data=pd.DataFrame(import_data).to_csv(
            index=False,
            encoding='utf-8',
            quoting=csv.QUOTE_NONNUMERIC
        )
    )
    return response.json()


async def main():
    """Retrieve schemas and import them into the Schemas table"""
    emx2 = requests.Session()
    current_schemas = await get_schemas(emx2, token=MOLGENIS_TOKEN)
    await post_schemas(client=emx2, data=current_schemas, token=MOLGENIS_TOKEN)


if __name__ == '__main__':
    asyncio.run(main())
