# About the Bulk Upload Template Generator

The generator creates an excel workbook containing one or more sheets that can be used for data entry. 

## Prerequisites

To run locally, you will need to create a `.env` file with the following credentials.

```txt
MOLGENIS_HOST=https://<my-emx2-instance>/
MOLGENIS_TOKEN=...
```

By default, the script uses the localhost which is the same if you have a local instance running or have deployed the script. If you want to use a specific instance, then define the `MOLGENIS_HOST`. Otherwise, you can leave this blank.

## Running locally

If you would like to test the script or further develop it, comment out the block that handles the `sys.argv`

```python
# process args: must send as a string separated with a ";"
# if len(sys.argv) >= 2:
#    ....
```

Then manually define a schema and tables.

```python
# init template builder params
SCHEMA: str = "my schema"
TABLES: list[str] = ['Table 1', 'Table 2',]
```

## Deploying the script

To use this script in your emx2 instance, follow these steps.

1. Create a new schema using the `PATIENT_REGISTRY` template with demo data.
2. In the new schema, go the "create page" page and create a new one. Open the page editor.
3. Create a new script and-
    i. copy the contents in the generator script into the script field
    ii. Copy the requirements.txt file into the requirements field
4. In the editor, copy the contents of the files in the `pages/Submission` folder into the respective editors (i.e., HTML file into the HTML editor, etc.).
5. Update the main parameters in the javascript: script name, targetSchema, and templates.

```js
// script name: the name of the generator py file in EMX2
const scriptName = "pet store generator";

// target schema: the name of the schema that contains the tables that you would like to generate a template
const targetSchema = "pet store";

// templates: a mapping between the form input and the tables (as a comma separated string)
// this is useful if you have a theme that corresponds to multiple tables 
const templates = {
    'Pets': 'Pet,Order'
}
```

If everything is correct, then the scripts should run as normally.
