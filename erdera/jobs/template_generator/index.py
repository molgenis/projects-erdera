"""Build excel template from schema"""

from os import environ
import sys
import logging
import xlsxwriter
from openpyxl.utils.cell import get_column_letter
from molgenis_emx2_pyclient import Client
from dotenv import load_dotenv
load_dotenv()

logging.captureWarnings(True)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger("Bulk Upload Template Generator: ")

# set params
HOST: str = environ['MOLGENIS_HOST']
TOKEN: str = environ['MOLGENIS_TOKEN']

SCHEMA: str = "RD3"
TABLES: list[str] = ['Individuals']

MAX_TEMPLATE_ROWS: int = 250


client = Client(url=HOST, token=TOKEN)


def check_for_ontology_types(meta) -> bool:
    """Determine if any column is an ontology type"""
    count: int = 0
    for col in meta:
        if col.columnType == 'ONTOLOGY':
            count += 1
    return count > 0


def create_template_worksheet(new_sheet, table_metadata, styles, lookup_sheet=None):
    """Create a new worksheet and add to workbook"""
    template_index: int = 0
    lookup_index: int = 0

    for column in table_metadata:
        log.info('Writing column %s into template...', column.name)
        tempate_index_letter: str = get_column_letter(template_index+1)
        is_key = column.key > 0 if column.get('key') else False
        is_required = column.get('required')

        if is_key is True or is_required is True:
            new_sheet.write(0,
                            template_index,
                            column.name,
                            styles['header']['required'])

            # apply cell styles for the first n rows
            for template_row_cell in range(1, MAX_TEMPLATE_ROWS):
                new_sheet.write(template_row_cell,
                                template_index,
                                None,
                                styles['cell']['required'])
        else:
            new_sheet.write(0,
                            template_index,
                            column.name,
                            styles['header']['default'])

        # determine if column is an ontology type and create lookups
        ontology_table = column.get("refTableName")
        if column.columnType.startswith('ONTOLOGY') and ontology_table is not None and lookup_sheet:
            ontology_schema: str = SCHEMA

            if bool(column.get('refSchemaId')):
                ontology_schema = column.refSchemaId

            log.info('Creating lookup for %s from %s...',
                     column.name,
                     ontology_schema)

            lookup_sheet.write(0,
                               lookup_index,
                               ontology_table,
                               styles['header']['default'])

            ontology = client.get(table=ontology_table,
                                  columns=['name'],
                                  schema=ontology_schema)

            for ontology_row_index, row in enumerate(ontology):
                lookup_sheet.write(ontology_row_index+1,
                                   lookup_index,
                                   row['name'])

            # set formula in template_sheet
            lookup_index_letter: str = get_column_letter(lookup_index+1)
            dv_source: str = f"=lookups!{lookup_index_letter}2:{lookup_index_letter}{len(ontology)}"

            for dv_row_index in range(1, MAX_TEMPLATE_ROWS, 1):
                new_sheet.data_validation(f"{tempate_index_letter}{dv_row_index+1}",
                                          {'validate': 'list', 'source': dv_source})
            lookup_index += 1
        template_index += 1


if __name__ == "__main__":
    log.info("Building template from: %s....", SCHEMA)

    # create workbook
    output_file: str = f"{SCHEMA}.xlsx"
    wb = xlsxwriter.Workbook(filename=output_file)

    # define workbook styles
    wb_styles = {
        'header': {
            'default': wb.add_format({'bottom': 1}),
            'required': wb.add_format({
                'bottom': 1,
                'bg_color': '#ADE1FF',
                'bold': True
            })
        },
        'cell': {
            'required': wb.add_format({
                'border': 1,
                'bg_color': '#f6f6f6'
            })
        }
    }

    wb_styles['cell']['required'].set_border_color("#cbcbcb")

    # retrieve schema metadata
    schema_meta = client.get_schema_metadata(name=SCHEMA)
    excluded_columnTypes = ['SECTION', 'HEADING']

    # build sheets
    lookups_sheet = None

    for table in TABLES:
        table_meta = schema_meta.get_table(by="name", value=table)
        cols_meta = [
            col for col in table_meta.columns
            if col.columnType not in excluded_columnTypes and not col.name.startswith('mg_')
        ]

        template_sheet = wb.add_worksheet(name=table)
        if check_for_ontology_types(cols_meta):
            lookups_sheet = wb.add_worksheet(name='lookups')
            create_template_worksheet(
                new_sheet=template_sheet,
                table_metadata=cols_meta,
                lookup_sheet=lookups_sheet,
                styles=wb_styles
            )
        else:
            create_template_worksheet(
                new_sheet=template_sheet,
                table_metadata=cols_meta,
                styles=wb_styles
            )

        template_sheet.autofit()

    # additional config
    lookups_sheet.autofit()
    lookups_sheet.protect()

    wb.close()
