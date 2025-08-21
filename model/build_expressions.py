"""Extract expressions from javascript files and insert them into the molgenis.csv"""
import asyncio
from os import listdir, environ
import re
from io import StringIO
from dotenv import load_dotenv
import pandas as pd
from molgenis_emx2_pyclient import Client
load_dotenv()


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
        'tableExtends': '',
        'tableType': '',
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


# async def upload_schema(path_to_molgenis: str):
#     """Upload the molgenis scheme with the expressions"""
#     # emx2 = Client(
#     #     'https://willemijn.molgenis.net',
#     #     schema='test',
#     #     token=environ['MOLGENIS_EMX2_TOKEN']
#     # )

#     async with Client('https://willemijn.molgenis.net', token=environ['MOLGENIS_EMX2_TOKEN']) as client:
#         # client.signin()
#         await client.upload_file(file_path=path_to_molgenis, schema='test')

#     # emx2.upload_file(file_path=path_to_molgenis, schema='test')


if __name__ == "__main__":
    EMX2_HOST = check_url_ending(environ['MOLGENIS_HOST'])
    host_schema_metadata = get_schema(
        EMX2_HOST,
        environ['MOLGENIS_HOST_SCHEMA']
    )

    js_files = get_js_files("./model/expressions/")

    schema_metadata = []
    for file in js_files:
        schema_metadata.append(extract_js_file_content(js_file=file))

    schema_metadata_df = pd.DataFrame(schema_metadata)

    # path_to_molgenis = '/Users/w.f.oudijk/Library/Mobile Documents/com~apple~CloudDocs/Documents/ERDERA/molgenis.csv'
    # schema_metadata_df.to_csv(path_to_molgenis, index=False)
    # asyncio.run(upload_schema(path_to_molgenis=path_to_molgenis))
