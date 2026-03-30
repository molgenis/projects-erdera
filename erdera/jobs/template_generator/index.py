"""Build excel template from schema"""

from os import environ
import sys
import logging
from typing import TypedDict
import xlsxwriter
from molgenis_emx2_pyclient import Client
from dotenv import load_dotenv
load_dotenv()

logging.captureWarnings(True)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger("EMX2 Template Builder")

SCHEMA: str = "pet store"
TABLE: str = "Pet"

client = Client(environ['MOLGENIS_HOST'], token=environ["MOLGENIS_TOKEN"])


class WorkbookStyles(TypedDict):
    """Workbook Header Format type"""
    header_default: dict
    header_required: dict


def check_for_ontology_types(meta) -> bool:
    """Determine if any column is an ontology type"""
    count: int = 0
    for col in meta:
        if col.columnType == 'ONTOLOGY':
            count += 1
    return count > 0


if __name__ == "__main__":
    log.info("Building template from: %s:%s....", SCHEMA, TABLE)

    # create workbook
    output_file: str = f"{SCHEMA} {TABLE}.xlsx"
    workbook = xlsxwriter.Workbook(filename=output_file)

    # define workbook styles
    wb_styles: WorkbookStyles = {
        'header_default': workbook.add_format({'bottom': 1}),
        'header_required': workbook.add_format({
            'bottom': 1,
            'bg_color': '#ADE1FF',
            'bold': True
        })
    }

    # retrieve schema metadata
    table_metadata = client \
        .get_schema_metadata(name=SCHEMA) \
        .get_table(by="name", value=TABLE)

    # reduce column metadata
    excluded_columns = ['SECTION', 'HEADING']
    tableColumnsMeta = [
        column
        for column in table_metadata.columns
        if column.columnType not in excluded_columns and not column.name.startswith("mg_")
    ]

    # init sheets
    template_sheet = workbook.add_worksheet(name=TABLE)
    lookups_sheet = workbook.add_worksheet(name="lookups")

    # create headers and lookups in spreadsheet
    index: int = 0
    lookupIndex: int = 0
    for column in tableColumnsMeta:
        log.info('Writing column %s into template...', column.name)

        # determine if column is an ontology type and create lookups
        ontologyTable = column.get("refTableId")
        if column.columnType.startswith('ONTOLOGY') and ontologyTable is not None:
            log.info(
                'Create lookup for %s from %s...', column.name, ontologyTable)

            ontSchema: str = SCHEMA
            if bool(column.get('refSchemaId')):
                ontSchema = column.refSchemaId

            ontology = client.get(ontologyTable, ['name'], schema=ontSchema)

            # write ontology to lookups sheets
            for ontRowIndex, row in enumerate(ontology):
                lookups_sheet.write(ontRowIndex, lookupIndex, row['name'])

            # set formula in template_sheet
            source_formula = f"=OFFSET(lookups!A1,0,COLUMN()*{lookupIndex},{len(ontology)},1)"
            template_sheet.data_validation(
                1, index, 250, index,
                {'validate': 'custom', 'source': source_formula})

            lookupIndex += 1

        # determine if column is a mandatory field and write to file
        is_key = column.key > 0 if column.get('key') else False
        is_required = column.get('required')

        if is_key is True or is_required is True:
            template_sheet.write(
                0, index, column.name, wb_styles['header_required'])
        else:
            template_sheet.write(
                0, index, column.name, wb_styles['header_default'])

        index += 1

    workbook.close()
