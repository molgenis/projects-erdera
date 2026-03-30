"""Build excel template from schema"""

from os import environ
import sys
import logging
from dotenv import load_dotenv
import xlsxwriter
from openpyxl.utils.cell import get_column_letter
from molgenis_emx2_pyclient import Client
load_dotenv()

logging.captureWarnings(True)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger("Bulk Upload Template Generator: ")

SCHEMA: str = "pet store"
TABLE: str = "Pet"
MAX_TEMPLATE_ROWS: int = 250

client = Client(environ['MOLGENIS_HOST'], token=environ["MOLGENIS_TOKEN"])


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
    styles_header_default = workbook.add_format({'bottom': 1})
    styles_header_required = workbook.add_format({
        'bottom': 1,
        'bg_color': '#ADE1FF',
        'bold': True
    })

    styles_cell_required = workbook.add_format({
        'border': 1,
        'bg_color': '#f6f6f6'
    })
    styles_cell_required.set_border_color("#cbcbcb")

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
    lookups_sheet = workbook.add_worksheet(name="_lookups")

    # create headers and lookups in spreadsheet
    TEMPLATE_INDEX: int = 0
    LOOKUP_INDEX: int = 0
    for column in tableColumnsMeta:
        log.info('Writing column %s into template...', column.name)

        TEMPATE_INDEX_LETTER: str = get_column_letter(TEMPLATE_INDEX+1)
        TEMPLATE_INDEX_CELL: str = f"{TEMPATE_INDEX_LETTER}:1"

        # determine if column is a mandatory field and write to file
        is_key = column.key > 0 if column.get('key') else False
        is_required = column.get('required')

        if is_key is True or is_required is True:
            template_sheet.write(
                0, TEMPLATE_INDEX, column.name, styles_header_required)

            # apply cell styles for the first n rows
            for template_row_cell in range(1, MAX_TEMPLATE_ROWS):
                template_sheet.write(
                    template_row_cell, TEMPLATE_INDEX, None, styles_cell_required)
        else:
            template_sheet.write(
                0, TEMPLATE_INDEX, column.name, styles_header_default)

        # determine if column is an ontology type and create lookups
        ONTOLOGY_TABLE = column.get("refTableId")
        if column.columnType.startswith('ONTOLOGY') and ONTOLOGY_TABLE is not None:
            log.info(
                'Creating lookup for %s from %s...',
                column.name,
                ONTOLOGY_TABLE
            )

            # set schema for ontology (either current or named) and retrieve data
            ONTOLOGY_SCHEMA: str = SCHEMA
            if bool(column.get('refSchemaId')):
                ONTOLOGY_SCHEMA = column.refSchemaId

            ontology = client.get(
                table=ONTOLOGY_TABLE,
                columns=['name'],
                schema=ONTOLOGY_SCHEMA
            )

            # write ontology to lookups sheets
            lookups_sheet.write(
                0, LOOKUP_INDEX, ONTOLOGY_TABLE, styles_header_default)

            for ONTOLOGY_ROW_INDEX, row in enumerate(ontology):
                lookups_sheet.write(
                    ONTOLOGY_ROW_INDEX+1, LOOKUP_INDEX, row['name'])

            # set formula in template_sheet
            LOOKUP_INDEX_LETTER: str = get_column_letter(LOOKUP_INDEX+1)
            DV_SOURCE_FORMULA: str = f"=_lookups!{LOOKUP_INDEX_LETTER}2:{LOOKUP_INDEX_LETTER}{len(ontology)}"

            for DV_ROW_INDEX in range(1, MAX_TEMPLATE_ROWS, 1):
                DV_CELL: str = f"{TEMPATE_INDEX_LETTER}{DV_ROW_INDEX+1}"
                template_sheet.data_validation(
                    DV_CELL, {'validate': 'list', 'source': DV_SOURCE_FORMULA})

            LOOKUP_INDEX += 1
        TEMPLATE_INDEX += 1

    # additional config
    template_sheet.autofit()
    lookups_sheet.autofit()

    workbook.close()
