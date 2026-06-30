## Mapping GPAP ERDERA data to RD3

Data providers submit phenotypic metadata to GPAP, this data is extracted and uploaded to RD3. **Participant** and **Experiment** metadata is extracted from GPAP.  

#### Mapping Participant metadata
The `mapping_cnag_to_rd3.py` script maps the CNAG (GPAP) participant data to RD3. 

The GPAP participant metadata is mapped to the following RD3 tables: 
- `Pedigree`
- `Individuals` 
- `Pedigree members`
- `Clinical observations`
- `Consent`
- `Disease history`
- `Phenotype observations`

> Note: Run this script before running the `mapping_cnag_experiments_to_rd3` script. You first need the individuals, 
> and then the corresponding experiments.

#### Mapping Experiment metadata
The `mapping_cnag_experiments_to_rd3.py` script maps the CNAG (GPAP) experiment data from the staging area to RD3. 

The GPAP experiments metadata is mapped to the following RD3 tables:
- `Resources` to capture: 
    - The `ERDERA` project
    - The `Solve-RD` project
    - An `incomplete families` resource to capture the families that are incomplete (e.g., missing an index case)
    - The data freezes: for example, `ERDERA_PF1`. 
- `Experiments srDNA`
- `Samples srDNA`

Uses `Experiment types`, `kits`, `Library source`, `Tissue types`, and `Erns` from `Ontology mappings` schema. 

#### Running the scripts (locally)
To run the scripts locally you need to create a `.env` file with the following parameters:
```txt
 MOLGENIS_HOST=https://<my-emx2-instance>/
 MOLGENIS_TOKEN=...
 SCHEMA_GPAP_SOURCE (The GPAP staging area)
 MOLGENIS_HOST_SCHEMA_TARGET (The erdera production database, the mapped data will be uploaded to this database)
 SCHEMA_ONTOLOGIES (The ontologies database, e.g., CatalogueOntologies)
 SCHEMA_ONTOLOGY_MAPPINGS (The `Ontology mappings` database)
 SCHEMA_JOBS (The `jobs` database)
 SCHEMA_QUALITY_CONTROL (The `Quality Control` database)
```