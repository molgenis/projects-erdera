"""Build excel template from schema"""

from os import environ
import sys
import logging
from typing import TypedDict
import textwrap

import xlsxwriter
from openpyxl.utils.cell import get_column_letter

from molgenis_emx2_pyclient import Client
from molgenis_emx2_pyclient.metadata import Schema, Table, Column
from dotenv import load_dotenv

load_dotenv()

logging.captureWarnings(True)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger("Template Generator")

# set defaults
MAX_TEMPLATE_ROWS: int = 1000
OUTPUT_FILE: str = environ.get('OUTPUT_FILE')
HOST: str = 'http://localhost:8080/'
if environ.get('MOLGENIS_HOST'):
    HOST = environ['MOLGENIS_HOST']

# init template builder params
SCHEMA: str = None # rd3
TABLES: list[str] = []

# process args: must send as a string separated with a ";"
if len(sys.argv) >= 2:
    print("args", sys.argv[1])

    args = sys.argv[1].split(";")
    SCHEMA = args[0].replace('\"', '')
    TABLES = args[1].split(",")

    ONTOLOGY_TAG = ""
    if len(args) > 2:
        ONTOLOGY_TAG = args[2]

    OUTPUT_FILE = f'{TABLES[0]}.xlsx'
    log.info('Received args: schema=%s, tables=%s, tag=%s',
             SCHEMA, TABLES, ONTOLOGY_TAG)


client = Client(url=HOST, token=environ['MOLGENIS_TOKEN'])


class WorkbookStyles(TypedDict):
    """Workbook formats"""
    header_required: dict
    header_default: dict
    cell_required: dict


class BuildTemplate:
    """Build template"""

    def __init__(self,
                 schema: str,
                 tables: list[str],
                 max_template_rows: int = 250,
                 sys_output_filename: str = None):
        """New template generator

        :param schema: name of the schema
        :type schema: str

        :param tables: names of the tables that exist in the schema
        :type tables: str[]

        :param max_template_rows: number of rows to prefill with styles, validation, etc.
        :type max_template_rows: int

        """
        self.output_filename = f"{schema}.xlsx"
        self.schema = schema
        self.tables = tables

        self.max_template_rows = max_template_rows

        self.should_build_lookup_sheet = False
        self.lookups_col_index = 0
        self.lookups = []

        if sys_output_filename:
            self.output_filename = sys_output_filename
            # output_basename = path.basename(sys_output_filename)
            # self.output_filename = sys_output_filename.replace(
            #     output_basename, self.output_filename)

    def column_is_required(self, column: Column) -> bool:
        """Determine if a column is required based on schema metadata

        :param column: metadata object for a column
        :type column: Column

        :returns: bool
        """
        is_key = column.key > 0 if column.get('key') else False
        is_req = column.get('required')
        if column.get('columnType') == 'AUTO_ID' and is_req:
            is_key = False
            is_req = False
        return is_key or is_req

    def rewrite_col_type(self,
                         column: Column):
        """
        Update the columnType to make its meaning clearer
        """
        columnType = column.get('columnType')
        refTableName = column.get('refTableName')
        if not columnType:
            columnType = 'STRING'
        elif columnType == 'DATE':
            columnType = 'DATE (yyyy-mm-dd)'
        elif columnType == 'STRING_ARRAY':
            columnType = "STRING_ARRAY: enter one or more values separated by a comma. E.g., 'value 1','value-2',..."
        elif columnType in ['ONTOLOGY', 'SELECT']:
            columnType = f'Select one item from {refTableName}'
        elif columnType in ['ONTOLOGY_ARRAY', 'MULTISELECT']:
            columnType = f'Select one or more items from {refTableName}'
        elif columnType == 'INT':
            columnType = 'INTEGER'
        elif columnType in ['BOOL']:
            columnType = 'Select TRUE or FALSE'
        
        return columnType
        
    def write_sheet_header(self,
                           sheet,
                           column: Column,
                           styles: WorkbookStyles,
                           col_index: int = 0):
        """Write a column header to a sheet

        :param sheet: a workbook.worksheet

        :param column: metadata object for a column
        :type column: Column

        :param col_index: the column to write the header into (zero index)
        :type col_index: int

        """
        current_header_style = styles['header_default']
        if self.column_is_required(column=column):
            current_header_style = styles['header_required']

        sheet.write(0, col_index, column.name, current_header_style)

        # add comment which will appear when hovered over the field
        columnType = self.rewrite_col_type(column=column)
        description = column.get('description')

        # comment text based on description and column type
        comment_text = f'{description}'
        if columnType:
            comment_text = f'{description} \n\n {columnType}'    

        # format comment
        width = 200
        # wrap the text to prevent text from falling outside the comment box
        wrap_at = 40
        wrapped_lines = []
        for line in comment_text.split("\n"):
            wrapped_lines.extend(textwrap.wrap(line, width=wrap_at))
        comment_text = "\n".join(wrapped_lines)
        # set height of comment box
        height = max(20, len(wrapped_lines) * 15)

        # write comment
        sheet.write_comment(0, col_index, comment_text,
                            {
                                'width': width,
                                'height': height
                            })

    def column_is_ontology_type_or_ref_to_orgs(self, column: Column) -> bool:
        """Determine if the column is ONTOLOGY, ONTOLOGY_ARRAY, or a reference to Organisations"""
        return column.columnType.startswith('ONTOLOGY') or column.get('refTableName') == 'Organisations'

    def table_has_ontology_types(self, table_meta: Table) -> bool:
        """Determine if there are ONTOLOGY types in a table"""
        count: int = 0
        for column in table_meta:
            if self.column_is_ontology_type_or_ref_to_orgs(column=column):
                count += 1
        return count > 0

    def build_sheet(self,
                    workbook, sheet_name: str,
                    column_metadata: list[Column],
                    styles: WorkbookStyles):
        """Build worksheet from schema metadata

        :param sheet_name: name of the new workbook sheet to create
        :type sheet_name: str

        :param column_metadata: column metadata from emx2 pyclient
        :type column_metadata: list[Column]
        """
        new_sheet = workbook.add_worksheet(name=sheet_name)
        index: int = 0
        for column in column_metadata:
            log.info('Processing column %s', column.name)
            # write header
            self.write_sheet_header(sheet=new_sheet,
                                    column=column,
                                    styles=styles,
                                    col_index=index)

            # determine if ontology table is present
            ontology_table: str = column.get('refTableName')
            should_build_ontology: bool = self.column_is_ontology_type_or_ref_to_orgs(
                column=column) and ontology_table is not None

            if should_build_ontology:
                lookups_col: str = get_column_letter(self.lookups_col_index+1)
                lookups_col_index = self.lookups_col_index
                lookup = next( # if lookup is already created, use this information
                    (elem for elem in self.lookups 
                     if elem['name'] == ontology_table), 
                     None)
                if lookup:
                    # get column letter and set template col to this range 
                    lookups_col = lookup['lookups_col']
                    lookups_col_index = lookup['lookups_col_index']
                    data = lookup['data']
                else: # if not, create the lookup list
                    log.info('Creating lookup from %s', ontology_table)
                    self.lookups_col_index += 1
                    self.should_build_lookup_sheet = True
                    ontology_schema: str = self.schema

                    if bool(column.get('refSchemaId')):
                        ontology_schema = column.refSchemaId

                    query_filter: str = ''
                    if ontology_table in [
                        'Concentration measurement type',
                        'File formats',
                        'Movietime',
                        'Sample type',
                        'Sequencing instrument models',
                        'Sequencing methods',
                        'Storage buffer',
                        'Storage conditions',
                        'Tissue type',
                        'Library source',
                        'Sequencing platforms',
                        'Units',
                        'Library layout'
                    ]:
                        query_filter = 'tags=="erdera"'
                        if ONTOLOGY_TAG != "":
                            query_filter = f"tags=='{ONTOLOGY_TAG}'"

                    data = client.get(
                        table=ontology_table,
                        columns=['name'],
                        query_filter=query_filter,
                        schema=ontology_schema)

                lookup = { # create lookup entry
                    'name': ontology_table,
                    'data': list(data),
                    'lookups_col': lookups_col,
                    'lookups_col_index': lookups_col_index,
                    'template_sheet': sheet_name,
                    'template_col': get_column_letter(index+1),
                    'template_col_index': index,
                    'formula': f"=lookups!{lookups_col}2:{lookups_col}{len(data)+1}"
                }
                self.lookups.append(lookup)
            
            # determine if column is a boolean
            is_bool: bool = column.get('columnType') == 'BOOL'
            # get the lookup column and index
            lookups_col: str = get_column_letter(self.lookups_col_index+1)
            lookups_col_index = self.lookups_col_index
            if is_bool:
                lookup = next(
                    (elem for elem in self.lookups 
                     if elem['name'] == 'Boolean'), 
                     None)
                # if there is already an boolean lookup, get the data (i.e., lookup col and index)
                if lookup:
                    # get column letter and set template col to this range 
                    lookups_col = lookup['lookups_col']
                    lookups_col_index = lookup['lookups_col_index']
                else: # only increment index if lookup is created for the boolean (first time) 
                    self.lookups_col_index += 1
                
                lookup = { # create lookup
                    'name': 'Boolean',
                    'data': [{'name':True}, {'name':False}],
                    'lookups_col': lookups_col,
                    'lookups_col_index': lookups_col_index,
                    'template_sheet': sheet_name,
                    'template_col': get_column_letter(index+1),
                    'template_col_index': index,
                    'formula': f"=lookups!{lookups_col}2:{lookups_col}3"
                }
                self.lookups.append(lookup)
                

            # iterate over rows in the sheet: apply styles and/or validation
            if self.column_is_required(column=column):
                for row_index in range(1, self.max_template_rows):
                    new_sheet.write(row_index,
                                    index,
                                    None,
                                    styles['cell_required'])

            index += 1
        new_sheet.autofit()

    def build(self, metadata: Schema):
        """Build template"""
        workbook = xlsxwriter.Workbook(filename=self.output_filename)

        # set workbook formats
        styles: WorkbookStyles = {
            'header_default': workbook.add_format({'border': 1}),
            'header_required': workbook.add_format({
                'bottom': 1,
                'bg_color': '#ADE1FF',
                'bold': True
            }),
            'cell_required': workbook.add_format({
                'border': 1,
                'bg_color': '#cbcbcb'
            })
        }
        styles['cell_required'].set_border_color('#cbcbcb')

        # build sheets before lookups
        for table in self.tables:
            log.info('Building sheet for %s', table)
            table_meta = metadata.get_table(by='name', value=table)

            excluded_types = ['SECTION', 'HEADING', 'REFBACK']
            col_meta = [
                col for col in table_meta.columns
                if col.columnType not in excluded_types and not col.name.startswith('mg_') and not col.get('visible') and not col.name == 'id'
            ]

            self.build_sheet(workbook=workbook,
                             sheet_name=table,
                             column_metadata=col_meta,
                             styles=styles)

        # only build lookups if present in the model
        if self.should_build_lookup_sheet:
            lookups_sheet = workbook.add_worksheet(name='lookups')
            written_columns = set() # to keep track of the lookup lists written to the lookups sheet
            for lookup in self.lookups:
                if lookup['lookups_col_index'] not in written_columns:
                    log.info('Creating lookup and apply validation rules for %s', lookup['name'])
                    lookups_sheet.write(
                        0,
                        lookup['lookups_col_index'],
                        lookup['name'],
                        styles['header_default'])

                    # write ontology terms
                    for index, row in enumerate(lookup['data']):
                        lookups_sheet.write(
                            index+1, lookup['lookups_col_index'], f"{row['name']}")
                    written_columns.add(lookup['lookups_col_index'])

                # apply validation always (also for the duplicate lookups)
                # apply validation in the appropriate sheet
                template_sheet = workbook.get_worksheet_by_name(
                    lookup['template_sheet'])
                if lookup['data']:
                    for row_index in range(1, self.max_template_rows):
                        template_sheet.data_validation(
                            f"{lookup['template_col']}{row_index+1}",
                            {'validate': 'list',
                                'source': f"{lookup['formula']}"}
                        )

            lookups_sheet.autofit()
            lookups_sheet.protect()
        workbook.close()


if __name__ == "__main__":
    log.info("Staring template generator on schema %s", SCHEMA)
    log.info('Sheets to create based on tables %s', TABLES)

    # retrieving metadata
    log.info('Retrieving schema metadata for %s', SCHEMA)
    schema_meta = client.get_schema_metadata(name=SCHEMA)

    # create new template generator and build
    template = BuildTemplate(
        schema=SCHEMA,
        tables=TABLES,
        max_template_rows=MAX_TEMPLATE_ROWS,
        sys_output_filename=OUTPUT_FILE
    )
    log.info('Building template.....')
    template.build(metadata=schema_meta)

    log.info('Saving file %s', template.output_filename)
