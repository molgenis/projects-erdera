"""Extract expressions from javascript files and insert them into the molgenis.csv"""
from os import listdir
import re
import pandas as pd


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
        'columnName': '',
        'js': ''
    }

    func_call = ''
    should_parse_js = False
    for line in contents:
        if "@tag" in line:
            schema_info = re.sub(r'(@|tag|\*|\n)', '', line).strip()
            parsed_js['tableName'] = schema_info.split(".")[0]
            parsed_js['columnName'] = schema_info.split(".")[
                1].replace("_", " ")
            parsed_js['expressionType'] = schema_info.split(".")[2]

        if re.match(r'^(export)', line):
            should_parse_js = True
            func_call = re.sub(
                r'(export|default|function|\n|\{)', '', line).strip()

        if should_parse_js is True:
            clean_js_line = re.sub(
                r'(export|default|\n)', '', line
            ).strip()
            parsed_js['js'] = parsed_js['js'] + clean_js_line

    parsed_js['js'] = f"{parsed_js['js']};{func_call}"
    return parsed_js


if __name__ == "__main__":

    js_files = get_js_files("./model/expressions/")

    schema_metadata = []
    for file in js_files:
        schema_metadata.append(extract_js_file_content(js_file=file))

    schema_metadata_df = pd.DataFrame(schema_metadata)
