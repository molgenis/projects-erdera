### Molgenis schemas

We define schemas to capture ERDERA-related data. 

#### Jobs
The `Jobs` database captures metadata about the jobs that retrieve GPAP and EGA metadata and map them to 
RD3. The database captures the following information: 
- `Jobs`: A generic table to capture general job information such as, run ID, date of run, information about errors (if any), etc
- `Jobs Rd3 mapping`: Extends `Jobs` and captures job information about the mapping of the GPAP and EGA metadata to RD3.
- `Jobs Gpap Api`: Extends `Jobs` and captures job information about the retrieval of the GPAP data using the API. 
- `Jobs Ega Api`: Extends `Jobs` and captures job information about the retrieval of the EGA data using the API. 

#### Lookups
ERN information is captured in the `lookups` schema. 

#### Ontology Mappings
The `ontology-mappings` database captures the lookup lists from GPAP (`incoming value` with optional `incoming code`) 
mapped to the corresponding ontology value (`new value`) of RD3. For `Diseases` and `Phenotypes` the `incoming code` is required as well.

If a `new value` is selected, the data will be mapped to this value after rerunning the mapping scripts. 

#### Quality Control
The `Quality Control` database captures mismatches between diseases and phenotypes ontologies used by GPAP and those used by RD3. For each mismatch, the GPAP ontology term and code are recorded alongside the corresponding value in RD3. The type of mismatch is also tracked, indicating whether it is a code mismatch or a name mismatch. 

The `is correct` field is a boolean flag that is set to False by default. When set to True, it indicates that the RD3 value is considered the correct mapping. This value will then be used when rerunning the mapping scripts. 

If the Rd3 value is not correct, an alternative mapping can be selected in the `correct disease` or `correct phenotype` field. The selected value will be used when the mapping scripts are rerun.

#### Staging Area Ega
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

#### Staging Area Gpap
This schema defines the `Staging Area Gpap` database. The database captures the GPAP data as-is. It contains the `Participant by experiment` and the `Experiment` GPAP metadata. 
