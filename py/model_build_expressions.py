"""Extract expressions from javascript files and insert them into the molgenis.csv"""
import asyncio
from os import listdir, environ, path
import re
import logging
from io import StringIO
import csv
import sys
import requests
from tqdm import tqdm
from dotenv import load_dotenv
import pandas as pd
from molgenis_emx2_pyclient import Client
load_dotenv()

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger("RD3:")


def check_url_ending(url: str):
    """If a URL does not end a forward slash, then add it"""
    return url if url.endswith("/") else f"{url}/"


def get_js_files(path_to_dir: str):
    """List Javascript files in a directory"""
    return [path_to_dir + file for file in listdir(path_to_dir) if '.js' in file]


def extract_js_file_content(js_file: str):
    """Extract lines that contain the javascript function"""
    with open(js_file, "r", encoding="utf-8") as js:
        contents = js.readlines()
    js.close()

    parsed_js = {
        'tableName': '',
        'columnName': ''
    }

    func_call = ''
    should_parse_js = False
    expression_type = ''

    for line in contents:
        if "@tag" in line:
            schema_info = re.sub(r'(@|tag|\*|\n)', '', line).strip()
            expression_type = schema_info.split(".")[2]
            parsed_js[expression_type] = ''

            parsed_js['tableName'] = schema_info.split(".")[0]
            parsed_js['columnName'] = schema_info.split(".")[
                1].replace("_", " ")

        if re.match(r'^(export)', line):
            should_parse_js = True
            func_call = re.sub(
                r'(export|default|function|\n|\{)', '', line).strip()

        if should_parse_js is True:
            clean_js_line = re.sub(
                r'(export|default|\n)', '', line
            ).strip()
            parsed_js[expression_type] = parsed_js[expression_type] + \
                clean_js_line

    func_call = re.sub(r'\(.*\)', f"({parsed_js['columnName']})", func_call)
    parsed_js[expression_type] = f"{parsed_js[expression_type]};{func_call}"
    return parsed_js


def get_schema(host: str, schema: str):
    """Retrieve the latest molgenis, in which the expressions need to be added"""
    with Client(host, token=environ['MOLGENIS_HOST_TOKEN']) as client:
        url = f"{host}{schema}/api/csv"
        response = client.session.get(url)
        schema_csv_string = StringIO(response.text)
        data = pd.read_csv(schema_csv_string, sep=",")
        return data


async def upload_schema(path_to_molgenis: str, host: str, schema: str, token: str):
    """Upload the molgenis scheme with the expressions"""
    async with Client(url=host, token=token) as client:
        await client.upload_file(file_path=path_to_molgenis, schema=schema)


if __name__ == "__main__":
    EMX2_HOST = check_url_ending(environ['MOLGENIS_HOST'])
    host_schema_metadata = get_schema(
        EMX2_HOST,
        environ['MOLGENIS_HOST_SCHEMA']
    )

    # parse js files and create molgenis.csv structure
    js_files = get_js_files("./src/js/")

    log.info('Extracting content from js files...')
    mg_expressions = []
    for file in tqdm(js_files):
        mg_expressions.append(extract_js_file_content(js_file=file))

    # retrieve schema metadata from remote and merge
    current_mg_schema = get_schema(
        environ['MOLGENIS_HOST'],
        environ['MOLGENIS_HOST_SCHEMA']
    )

    log.info('Adding expressions to schema...')
    for row in tqdm(mg_expressions):
        matching_row = current_mg_schema.loc[(
            (current_mg_schema['tableName'] == row['tableName']) &
            (current_mg_schema['columnName'] == row['columnName'])
        )]

        if len(matching_row.index):
            for column in ['computed', 'required', 'validation', 'visible']:
                if column in row:
                    current_mg_schema.loc[(
                        (current_mg_schema['tableName'] == row['tableName']) &
                        (current_mg_schema['columnName'] == row['columnName'])
                    ),
                        column
                    ] = row[column]

        else:
            matching_row_err = f"Expression is defined, but {current_mg_schema['tableName']}.{current_mg_schema['columnName']} does not exist"
            log.error(matching_row_err)

    # import
    updated_mg_schema_df = pd.DataFrame(current_mg_schema)
    updated_mg_schema_df['key'] = updated_mg_schema_df['key'].astype('Int64')
    updated_mg_schema_df.to_csv(
        './tmp/molgenis.csv', index=False, quoting=csv.QUOTE_ALL)

    mg_schema = environ['MOLGENIS_HOST_SCHEMA']
    try:
        UPLOAD_SCHEMA_SUCCESS = f"Updated schema metadata in {mg_schema}"
        log.error(UPLOAD_SCHEMA_SUCCESS)
        asyncio.run(
            upload_schema(
                path_to_molgenis=path.abspath('./tmp/molgenis.csv'),
                host=environ['MOLGENIS_HOST'],
                schema=mg_schema,
                token=environ['MOLGENIS_HOST_TOKEN']
            )
        )
    except requests.exceptions.HTTPError as err:
        mg_schema = environ['MOLGENIS_HOST_SCHEMA']
        UPLOAD_SCHEMA_ERROR = f"Failed to import schema metadata into {mg_schema}"
        log.error(UPLOAD_SCHEMA_ERROR)
