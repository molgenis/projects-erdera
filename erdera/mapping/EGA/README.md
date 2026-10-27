## Mapping EGA metadata to RD3
GPAP submits metadata to the EGA, these scripts extract and uploaded these data to RD3 for a user-specified EGA dataset using the EGA dataset accession ID. 

#### Staging area
First retrieve the EGA metadata from the API and import into the EGA staging area (`mapping_ega_to_staging.py`).   
See specification of the EGA metadata API [here](https://metadata.ega-archive.org/spec/#/).

> [!NOTE]
> Please note that before uploading the metadata from the `files` and `dataset` endpoints, you must first upload the metadata from the remaining datasets endpoints of an EGA dataset.
> The mapping scripts require the information from these endpoints, such as `analyses`, `analysis_sample`, `sample_file`, `samples`, `studies`, `study_analysis_sample`. 
> This requires access to the EGA dataset. The zipped metadata archive should be uploaded to the EGA staging area. 

#### Mapping EGA data to RD3
Second, map the EGA metadata from the staging area to RD3 (`mapping_ega_to_rd3.py`). 

- Files: the EGA file metadata is mapped to the RD3 `Files` table
- Collections: the EGA study and dataset information is mapped to the RD3 `Collections` table

#### Running the scripts (locally)
To run the scripts locally you need to create a `.env` file with the following parameters:
```txt
 MOLGENIS_HOST=https://<my-emx2-instance>/
 MOLGENIS_TOKEN=...
 MOLGENIS_HOST_SCHEMA_TARGET (The erdera production database, the mapped data will be uploaded to this database)
 SCHEMA_JOBS (The `jobs` database)
 PROVISIONAL_ID (The EGA provisional ID of the dataset you want to map to RD3)
 SCHEMA_EGA_SOURCE (The EGA staging area)
```

#### Deploying the scripts
To run the `mapping_ega_to_staging` script remotely, you need to 

1. Copy the contents of [this script](mapping_ega_to_staging.py) into the script editor UI
2. Add the egaClient.py as an extra file and adjust the import so it imports from the file
3. Copy the following requirements into the 'dependencies' field
4. Save the script
5. Run the script, ensure you enter the EGA dataset accession ID of the data you want to migrate

And to run the `mapping_ega_to_rd3` script remotely, you need to

1. Copy the contents of [this script](mapping_ega_to_rd3.py) into the script editor UI
2. Uncomment the job parameter in the client initialisation in the main function
3. Copy the following requirements into the 'dependencies' field
4. Save the script
5. Run the script, ensure you enter the EGA dataset accession ID of the data you want to migrate

#### Dependencies

pandas  
python-dotenv  
molgenis-emx2-pyclient  