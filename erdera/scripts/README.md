## Scripts 

### GPAP
#### mapping_cnag_to_rd3
This script maps the CNAG (GPAP) participant data to RD3. 

The GPAP participant metadata is mapped to the following RD3 tables: 
- `Pedigree`
- `Individuals` 
- `Pedigree members` (disabled at the moment)
- `Clinical observations`
- `Consent`
- `Disease history`
- `Phenotype observations`

> Note: Run this script before running the `mapping_cnag_experiments_to_rd3` script. You first need the individuals, 
> and then the corresponding experiments.

#### mapping_cnag_experiments_to_rd3
This script maps the CNAG (GPAP) data from the staging area to RD3. 

- `Agents`
- `Endpoints`
- `Resources`
- `NGS Sequencing` (based on experiments): the GPAP Experiment metadata is mapped to `NGS Sequencing`

Uses `Experiment types`, `kits`, `Library source`, `Tissue types`, and `Erns` from `Ontology mappings` schema. 

### EGA
Mapping EGA metadata to RD3: 
1. First retrieve the EGA metadata from the API and import into the EGA staging area (`mapping_ega_to_staging`).
2. Map the EGA metadata from the staging area to RD3 (`mapping_ega_to_rd3`). 

#### mapping_ega_to_staging
Maps the EGA metadata to the staging area on EMX2. The script retrieves metadata from the following EGA endpoints: 

See specification of the EGA metadata API [here](https://metadata.ega-archive.org/spec/#/).

#### mapping_ega_to_rd3 
Maps the EGA metadata from the staging area to RD3 

- Files: the EGA file metadata is mapped to the RD3 `Files` table
- Resources: the EGA study and dataset information is mapped to the RD3 `Resources` table

### Molgenis schemas
#### ontology-mappings/molgenis
This schema defines the `ontology-mappings` database. This database contains the lookup lists from GPAP (`incoming value` with optional `incoming code`) 
mapped to the corresponding ontology value (`new value`) of RD3. 

`Diseases` and `Phenotypes` contain only the values that could not be mapped, i.e., that do not have a(n) (obvious) RD3 match. 
These values need to be manually checked to see whether there is an RD3 match, otherwise an ontology value can be added.

#### staging_area_ega/molgenis
This schema defines the `Staging Area Ega` database. The database captures the EGA data as-is. It contains the EGA metadata from the following endpoints: 
- /datasets/{accession_id}
- /datasets/{accession_id}/analyses
- /datasets/{accession_id}/samples
- /datasets/{accession_id}/experiments
- /datasets/{accession_id}/runs
- /datasets/{accession_id}/studies
- /datasets/{accession_id}files
- /datasets/{accession_id}/mappings/study_experiment_run_sample
- /datasets/{accession_id}/mappings/run_sample
- /datasets/{accession_id}/mappings/study_analysis_sample
- /datasets/{accession_id}/mappings/analysis_sample
- /datasets/{accession_id}/mappings/sample_file

#### staging_area_gpap/molgenis
This schema defines the `Staging Area Gpap` database. The database captures the GPAP data as-is. It contains the `Participant by experiment` and the `experiment` GPAP metadata. 

#### jobs/molgenis
This schema defines the `Jobs` database to capture metadata about the jobs that retrieve GPAP and EGA metadata and map them to 
RD3. The database captures the following information: 
- `Jobs`: A generic table to capture general job information such as, run ID, date of run, information about errors (if any), etc
- `Jobs Rd3 mapping`: Extends `Jobs` and captures job information about the mapping of the GPAP and EGA metadata to RD3.
- `Jobs Gpap Api`: Extends `Jobs` and captures job information about the retrieval of the GPAP data using the API. 
- `Jobs Ega Api`: Extends `Jobs` and captures job information about the retrieval of the EGA data using the API. 
