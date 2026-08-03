# About the Migrator

The migrator publishes ERDERA data from the staging areas to the production RD3 database. 

The script processes al schemas named `Staging area <institute>`, with the exception of `Staging area GPAP`, `Staging area EGA`, and `Staging area ids`. 

For each staging area, the data is exported and filtered to remove molgenis-related and empty files, leaving only the submitted data. If any draft records are detected, the migration for that staging area is skipped and a message is logged. 

When data is available for migration, the filtered export is packaged into a zipped archive and uploaded to the production database. After a succesful upload, the migrated data will is removed from the staging area.

## Prerequisites

To run locally, you will need to create a `.env` file with the following credentials. 

```txt
MOLGENIS_HOST=https://<my-emx2-instance>/
MOLGENIS_TOKEN=...
```

By default, the script uses the localhost which is the same if you have a local instance running or have deployed the script. If you want to use a specific instance, then define the `MOLGENIS_HOST`. Otherwise, you can leave this blank.

## Running locally

Example on how to run the script locally.

```python
# source venv/bin/activate
python erdera/migration/migrator.py
```

## Deploying the script

To use this script in your emx2 instance, follow these steps.

1. Go to the server where you want to deploy the script.
2. Go to the `scripts` overview and create a new script.
3. Copy the contents of the [Migrator script](migrator.py) into the script field.
4. Add the following dependencies.
```txt
molgenis_emx2_pyclient
pandas
python-dotenv
```
5. Run the script. Make sure to check the log file with output of the script.
