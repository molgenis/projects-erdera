## Mapping EGA metadata to RD3
GPAP submits file metadata to the EGA, this data is extracted and uploaded to RD3. 

#### Staging area
First retrieve the EGA metadata from the API and import into the EGA staging area (`mapping_ega_to_staging.py`).   
See specification of the EGA metadata API [here](https://metadata.ega-archive.org/spec/#/).

#### Mapping EGA data to RD3
Second, map the EGA metadata from the staging area to RD3 (`mapping_ega_to_rd3.py`). 

- Files: the EGA file metadata is mapped to the RD3 `Files` table
- Resources: the EGA study and dataset information is mapped to the RD3 `Resources` table

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